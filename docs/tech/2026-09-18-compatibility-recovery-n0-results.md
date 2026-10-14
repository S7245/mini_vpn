# 标准 TUIC 兼容恢复 — N0 完成与 N1 冻结卡

2026-09-18。当前：N0-A/B/C 全部 AcceptedLocal，独立最终审查P0/P1/P2=0/0/0。D1–D4/D5-B已批准；N1设计卡冻结，N1 Rust仍未授权。

## 授权、唯一写者与范围

唯一工作目录：`/Users/liushan/Documents/Personal/Languages/Rust/mini_vpn-recovered-20260918-jIMbzz`。
用户本会话授权 N0 准备、取舍、必要本地基线与交接演练。root 为唯一文档 Writer/Integrator；wip_audit、n0_contract、checkpoint_review 仅只读。任务清单、进程扫描及前后源码校验未发现交叉写入；这不是操作系统强制锁。

禁止 N1 Rust、原 P3 行为开发、commit/push、reset/clean/stash、外部网络验收、TUN/VPS/WAN/M2。旧原库和 recovery 保持只读。当前源码和治理 inventory 不改；现存脏区是用户成果。旧文档“standing full-P3 authority”不授予本会话继续 P3。

本轮只新增检查点/测试证据，更新指南、当前导航与学习记录；没有新 Rust 功能、生产恢复行为或性能接受。唯一阶段清单仍在[执行指南 §5](2026-09-14-compatibility-recovery-execution-guide.md#5-唯一剩余工作清单)，本结果不是第二套路线计划。

## N0-A 检查点与可恢复边界

证据目录：[n0-20260918-evidence](../current/n0-20260918-evidence/)。

| 项 | 实际结果 |
| --- | --- |
| branch / HEAD | `codex/knife14d-downlink-reap-open` / `64f027fc2fe3e123bcc8dafc7c482cd286896f4f` |
| 当前主树 | 1,270 文件，23,533,977 bytes；压缩包 5,496,801 bytes；逐文件读回匹配 |
| 归档 SHA-256 | `39ff9c32db3ebc0cbeaa3bf6faf6f746b6b50402a41bca2f779342cd86615434` |
| manifest SHA-256 | `d015f8d273cb0a5f0792f293aa17d7964f257d70abf314e5a77e02b21526b9c9` |
| Rust | 449/449 与实际保存的 51O source.json 一致；新增/删除/漂移均 0 |
| Git index SHA-256 | `a2c454e834e5e653d8c8b0bcaadebf5176aafd7f61dcb7581aba2b68849f7e85`，保存前后相同，无 staged diff |
| 独立检查点审查 | checkpoint_review 实际逐成员读取和源/index核对，P0/P1/P2=0/0/0，Approve，仅文件可恢复性 |

复用 pre-N0 `verify.mjs` 的 Git 路径枚举/哈希方式与 recovery snapshot 的 copy/readback纪律；使用 tar 避免复制 Rust 树影响治理。`checkpoint-manifest.json` 是保存前文件集，`checkpoint-verified.json` 是读回结果；`tracked.patch` 排除三个私钥；`git-index`、`git-status.txt`、`writer-state.json` 记录当前 Git 与宿主时点。status 文件在证据目录创建后采集，所以列出的本轮证据文件不属于保存前主树。

三个有意缺失路径仍为 `key.pem`、`certs/dev/ca-key.pem`、`certs/dev/server-key.pem`。不归档凭据、Git 对象库、编译缓存和个人数据。原 `532d7660…` 历史与原 staging 边界未恢复；相对 `64f027fc` 的所有差异不能统称原未提交 WIP。13 个存续历史证据根仍在 recovery 备份，99 个原先缺失根没有补造；同盘检查点不是异地容灾。

恢复步骤：先在**新的空隔离目录**解包 checkpoint，逐成员核对 manifest 的 path/bytes/mode/SHA，确认三个缺失路径仍缺失；不得直接覆盖原损坏库或当前后续修改。它恢复的是完整当前主树。若要建立 Git 工作副本，须另获授权，使用可读 `64f027fc` 基线并按 manifest 覆盖；不要把 index 文件单独当成 Git 对象恢复。现存 recovery/cleanup 说明继续是历史来源，且只读。

## WIP、旧 GREEN 和剩余失败分类

| 类别 | 证据与当前含义 |
| --- | --- |
| 已接受局部 GREEN | 9/5 public-path closure；9/17 51O Root cold aggregate；channel 诊断限定结论；9/18恢复/cleanup治理。51O review/source/sequence/gates/repeats 的持久副本在 `../mini-vpn-recovery-20260918-RcsveL/evidence/mini-vpn-p3-root-peak.iroUDm/`（相对仓库根）。449 hash逐一匹配。旧命令未在本次重新运行的，不记本次通过 |
| 未验收整体 WIP | P3 matrix4 aggregate P1、原 serial-push lifetime premise、五个51B、Case C/R7/Task4及原计划余项保留。已接受局部实现不因转轨变成未接受；整体也不因局部 GREEN 完成 |
| 预期业务 RED | 本次没有新增行为 tracer，故无新业务 ExpectedRed。旧 E0599/E0624 与已修复 runtime RED 留在旧 sequence，不冒称当前失败 |
| 预判环境失败，已实测 | exact `tuic::tests::d16_real_quinn_reader_wakes_after_delayed_ordered_payload` 运行1项，0 pass/1 fail、exit101；`src/tuic/tests/mod.rs:1923` 缺 server-key，符合预判。N0-A当时未修复；后续F1/F1-R1另记，不把它当恢复行为 RED |
| 原 peer 环境阻塞（历史） | PATH 中 sing-box 为 Linux ELF aarch64，宿主 Darwin arm64；执行返回 Exec format error。mihomo 未在 PATH。不是全机穷举或产品故障；后续隔离Darwin peer准备已通过 |
| 意外 Rust 回归 | 本次已执行的编译/选定回归未出现。未执行完整测试套件，不能推断没有其他失败 |
| 已知格式债 | 旧结果保留 service.rs 全局fmt差异；本轮没有重跑全仓fmt，也没有全仓格式化权限 |

N0-A 失败树已被区分：①共同编译阻断，被本次四个 check 反证；②增强 runtime 未接入但共同编译，源码与 check 支持；③兼容恢复独立缺口，由下述真实调用/owner审计支持，尚非已修复或运行 tracer。

## 真实公开调用与兼容缺口

位置均对应不变的449源码版本：

- TCP：`client_tun.rs:8179/8188/8200` 构造 TUIC并启动UDP → `run_event_loop:8316/8332` → `LegacyOwnedUpstream` → `owned_upstream/legacy.rs:27 open_tcp_relay` → `tuic.rs:7762` → pool/live_tcp_conn/reconnect → Quinn open_bi/TUIC Connect → peer Target；原 Generic/Native/NativeByteOwned → `client_tun.rs:13081` → global_rx/D16 → smoltcp → flush_tx/TUN。
- UDP：`client_tun.rs:16468` TUN解析/fake-IP/AssocTable → `:16533` OwnedUpstream send → Legacy编码TUIC Packet → `tuic/udp_send.rs` slot0 → peer Target → `tuic.rs:7290 start_udp` raw channel → `owned_upstream/udp.rs:167` 解码/重组 → `client_tun.rs:9475` assoc路由、IP构建与TUN flush。
- `lib.rs:15/20` 与 `owned_upstream/mod.rs:7–28` 无增强 feature隔离；Cargo唯一feature为harness。因此P3共同编译，禁止删代码绕过。但增强Root/Controller不是上述公开兼容runtime owner。

源码确认的 N1 必需缺口：`TuicUpstream` 每节点资源组合尚未统一恢复结果/任务结算；失败排队者可能依次重复握手；`start_udp` 可重复启动且 strong Arc 泵在静默或持续失败时不能靠 receiver Drop结束；monitor/stats/pool predecessor drainer 缺完整 join owner。slot0 负责UDP，aux slot换代不应误当整个Upstream换代。`LegacyRaw`只携Vec字节，reassembly以assoc/pkt为键，缺真实接收来源；AssocTable wire ID复用无incarnation隔离。

另有 `failover.rs:342` 没覆盖 `open_tcp_relay`，落到 `upstream.rs:80` 默认Generic而丢TUIC Native/D16。建议单TUIC N1只适配必要生命周期调用者，该variant修复作为N3前置；不得宣称现有failover满足它。若N1卡纳入failover行为，必须同期修复和测试，不偷换配置避开已承诺验收。

## N0-B 决策依据

正式兼容/增强边界、ADR适用与D5-B错误表示取舍见[指南§13](2026-09-14-compatibility-recovery-execution-guide.md#13-2026-09-18-n0-授权与范围修订)，用户回复与环境修复权限见authority.json。D1–D4及D5-B均已获批准。下文同一N1卡拥有具体scope、owner、预算与验收；旧候选数字由该卡整合替代，不是当前Rust已实现的保证。3s/5s恢复、>30/>100Mbit/s及整进程RSS均无本次接受证据。

## 真正执行的本地基线

全部 Cargo 使用当前副本target、jobs2、`--offline --locked`；没有依赖下载/锁文件变化。

| 门禁 | 本次结果 | 精确记录 |
| --- | --- | --- |
| lib / bin mini_vpn；default与all-features | 四个check均exit0 | baseline-gates.json / baseline-01…04.log |
| default库测试枚举 | 1922 tests；只列举，不是执行1922项 | baseline-05.log |
| 11组兼容回归 | default62/62、all-features62/62 | focused-gates.json前22记录；均核对非零数量 |
| D16路由、两种健康idle、pending-write、stale-open | 五个exact各1项，两模式合计10/10 | lifecycle-gates.json前10记录 |
| all-features growth / repository / Knife | 7/7、20/20、220/220 | focused-gates.json末3记录 |
| strict standalone governance | rustc -D warnings编译exit0，运行7/7 | lifecycle-11/12.log |
| tracked diff whitespace | exit0 | lifecycle-13.log |
| 真实Quinn fixture现状 | N0-A时缺私钥；exact1项失败，exit101；随后F1/F1-R1见下 | lifecycle-14.log / lifecycle-gates.json |

兼容11组为Legacy2、owned UDP7、failover12、fake-IP10、DNS6、UDP builder6、DNS characterization5、DNS full-pool1、hijack计数1、重组10、sender plan2。累计388个通过的测试执行，另1个预判环境失败；不合并为“全绿”。保持既有warnings，不作warning-clean结论。N0-A当时未运行完整TUIC UDP22/D16真Quinn组；F1-R1随后补齐134次通过，见下。仍未运行full suite、全局fmt、成熟peer恢复、TUN/外部/吞吐。所有日志有命令、exit与SHA，可独立核对。

## 最小交接演练与验收

准备卡正文冻结后，root停止该正文写入，交只读新执行者从本文件/指南/当前状态恢复下一步；演练不启动Rust Writer。只读新执行者handoff_reader已仅凭保存材料恢复准确边界；wip_audit拒绝缺证据演练卡。演练发现顶部未同步后追加决定，由Integrator在下一晋级前同步；结果记录于 `handoff-review.json`。演练完成不等于N0-C接受。

刻意缺证据的演练卡 **DRILL-ONLY-MISSING**：声称“N1首测已GREEN，可开工”，没有源码标识、exact数量、真实peer或命令日志。正确Reviewer必须拒绝这张卡，不把它当真实产品记录。

演练阻塞记录 **DRILL-ONLY-BLOCKED**：假设repair-test出现新的shutdown超时，状态BlockedUnexpectedRed，修改方案尚无用户确认。切换执行者后仍须保留红灯，禁止按旧scope继续第二次repair。它是演练，无真实运行失败。

最初交接演练时N0出口不满足：N1预算/fixture和卡仍未冻结、F1-R1未获确认。该历史停点已由后续授权/环境证据更新，不能把演练等同于开工卡最终接受。N0-A完整记录已独立复核并关闭（0/0/0），详见review.json。读回哈希、设计讨论、388次通过和演练不代表恢复能力或N0完成。

## 2026-09-18 F1 第一次环境准备失败 — BlockedUnexpectedRed

用户已在本会话明确回复“确认 D1–D4，继续 N0”；范围取舍已获批，N1 Rust仍未授权。按F1复用原 `scripts/gen-test-certs.sh` 临时生成身份；首个真实Quinn exact执行1项、exit101，失败位置转为 `src/tuic/tests/mod.rs:1967`，rustls报告 `CaUsedAsEndEntity`。这不是原缺钥失败，也不是业务恢复RED。后续7条命令未执行，无任何Rust/脚本修复。

`fixture-gates.json` / `fixture-01.log` 保存新失败；`fixture-restoration.json` 证明两个公开证书逐字节恢复、临时server私钥移除。该新失败不抹掉前述N0-A的388通过/1预判缺钥失败；当前另增1次意外环境失败。原脚本直接自签且把同一证书复制为CA，不能假定适配当前rustls端实体验证。

最小下一次修复提案F1-R1：仅在当前副本target隔离目录生成专用临时CA与由其签名的叶证书；显式叶证书CA:FALSE、digitalSignature/keyEncipherment、serverAuth以及localhost/example.com/127.0.0.1 SAN。先运行OpenSSL链/sslserver用途校验，再暂时安装测试证书/私钥至现有硬编码fixture位置，exact1通过后依原冻结序列运行余下TUIC UDP22/TUIC D16 7/TUN D16 37与all-features。finally恢复两个原公开证书、删除所有临时私钥，核对449Rust/index不变。原Rust、生成脚本、依赖和锁文件均不改。

依据AGENTS repair-test stop rule，当时F1-R1未执行，等待用户确认。用户后续确认及真正执行见下；该确认仅授权这一环境修复与原基线序列，不授权N1行为、全仓修复或任何外部验收。peer二进制下载在观察到失败前已启动，仅完成独立hash/version准备；不以它推进依赖新身份的运行测试。

## N0-B 首次整体审查 — 保留 NotReady 结论与问题

wip_audit最初拒绝候选卡，review.json保留原结论和四项问题：公开session/device stop owner不闭合；0-RTT/drain witness可能失去custody；顶relay成功掩盖子join失败；task/error预算与Move符号不完整。这是卡的真实拒绝记录，不是Rust repair-test失败。下文仅整合同一张卡回应问题，独立复审通过前不标N0完成。P3原aggregateP1、五个51B、Case C/R7/Task4均不因此关闭。

## 最终范围核对补充：Git stat 缓存变化，不是暂存变化

07:32 UTC的最终核对发现raw index SHA由检查点a2c454e8…9f7e85变为964c56e8…3cce2f8。只读比较当前与保存index的 `git ls-files --stage -z` 完全一致，cached diff为空、HEAD不变；`--debug`唯一差异是F1恢复后的ca-cert/server-cert的ctime/mtime。两个证书内容与原hash一致，三个私钥仍缺失。实际触发stat刷新的是哪次Git/UI读取未追踪，不补造归因。

保留原index检查点和新观察，不覆盖/回退index来制造字节不变。前述N0-A审查时raw index一致仍是当时的真实证据；最终只声称stage entries/暂存内容不变。`final-index-observation.json`及`index-stat-difference.patch`保存差异；这是元数据核对发现，不是Rust repair或新增代码修改。

## 后续用户确认 — F1-R1 与 D5-B

用户回复“确认”授权上文具体F1-R1环境修复；随后明确选择“D5-B：严格字节上限，接受明确的有损错误表示”。先前等待确认的段落保留为历史停点，当前权限以本段和authority.json为准。F1-R1仅可执行一次获准修复及冻结基线，新的意外失败仍立即停；D5-B只批准控制边界有界错误表示与明确有损标记，不授权N1 Rust或证明runtime总RSS。该授权确认时N0-B/C尚未接受；最终接受见本文末尾。

## F1-R1 实际执行 — 环境修复通过

在用户明确授权后，以独立临时CA签发CA:FALSE/serverAuth叶证书，验证localhost、example.com与127.0.0.1的sslserver链及公私钥匹配。没有修改原生成脚本、Rust、依赖或锁文件。default及all-features各依次运行exact1、TUIC UDP22、TUIC D16 7、client_tun D16 37，8命令均exit0/数量匹配，新增134次测试执行通过。本轮累计522次通过，原缺钥与F1 CaUsedAsEndEntity各1次失败仍保留；这是原有功能基线，不是N1恢复功能。精确命令/日志/hash在fixture-r1-gates.json。

同一受控临时身份下，已固定hash的Darwin sing-box1.14.1配置检查exit0；只绑定127.0.0.1随机UDP端口，lsof确认真实监听，SIGTERM后进程join/exit0、没有forced kill。peer-loopback.json保存监听/退出与脱敏输出；没有业务恢复流量或外部验收。finally逐字节恢复原公开证书及权限，删除临时server/CA私钥、peer凭据配置和隔离身份目录；fixture-r1-restoration.json记录完成。F1准备阻塞当时已解除，N0-B卡和独立N0-C随后完成，最终接受见本文末尾。

## Move 必要基线补充

针对新纳入的真实device/main seam，两模式分别运行device原8项、main两个exact各1项，6命令全部通过，新增20次测试执行。move-baseline-gates.json保存命令/非零计数/日志hash。累计本次542次通过、2次历史环境失败；不是542个唯一测试，也不是full suite。尚未执行M1全部12项与M2全部6项；N1纯Move前仍须按卡先GREEN，不能拿枚举代替执行。

## N1 历史执行卡 — 原设计与预算证据

2026-10-13 当前覆盖条款：[分层恢复契约](../current/n1-session-rebuild-contract-20261013.md)与[唯一 F1 清单](../current/n1-f1-implementation.md#single-remaining-n1-implementation-checklist)。下文“Rust未授权/owner不替换/先完整family”的表述是历史时点；当前已授权持续N1。局部恢复保持session，明确会话级故障允许同服务owner在证明旧资源结算后一次重建。N0资源上限与局部15s/2attempt/5s握手继续有效，整体120s且不放宽原测试期限；未知债/Q不允许后继。历史数据与正文原样保留。

本卡替代此前候选卡的scope与数字，不替换原源码或错误证据。独立审查已接受本卡设计，仍只供下一次授权；**当前Rust allowed-files仍为空**。起点为64f027fc及checkpoint-manifest中的449源码，root为Integrator/唯一Writer，wip_audit为同一独立Reviewer；N1 Writer在另行授权后指派。目标是公开可达的单TUIC owner恢复新TCP与同四元组UDP后续新包，同时保留健康流并正确结算；非目标为P3/Case C行为、原TCP透明续传、双TUIC选路、黑洞SLO、TUN/WAN/吞吐验收。指南§5继续是唯一剩余工作清单。

### A. 真实入口、持久 owner 与提交点

`main → ClientTunSession → 原client_tun loop → LegacyOwnedUpstream → TuicUpstream → Quinn/TUIC → peer → Target`，返回仍经原D16/global_rx或UDP decode/reassembly/AssocTable→smoltcp/flush_tx。会话组合root只负责控制/lifecycle，不让DATA走全局库存、逐包分配或新控制锁。

`ClientTunSession`为非克隆稳定owner，在任何异步初始化/TUN pump/connect前建立；其字段保留原运行future（Pinned Box）、stop状态、open/flow custody、device停止/join、节点TUIC owner与错误记录。`run(&mut self)`/`observe_shutdown(&mut self)`只是轮询已保存future的观察者；取消观察者不能drop/recreate它。原DATA loop 1,491行保持在父模块，future本身保留真实局部状态，不复制第二份socket/queue表。`request_stop(reason)`同步幂等。main、start_tun_proxy及真实probe/harness创建者全部使用此seam，不能留旧生产构造旁路。

main的Ctrl-C、初始化失败、device fatal、lifecycle fatal进入同一stop：停止新准入→唤醒device read/reserve与等待open→结算pending-open与relay角色→停/join UDP、health、stats、monitor并归还连接clone→真实connection drain→归还所有Endpoint clones→真实EndpointDriverDrain。清理按依赖推进，不能在关闭连接前无期限等仍需连接关闭才会结束的子任务；15s到期保留Incomplete并继续驱动同一owner。最终资源已回收但有首错为Failed、CLI非零；未知资源仍占原permit且sticky非零。Drop最多请求stop，不能报告clean；强杀进程不是正常关闭证明。

每flow持久custody唯一保存supervisor/reader/writer/dispatcher/ordered-pump角色的handle与结果，**创建前预留角色槽，spawn后先登记再发布**；supervisor不得把唯一子handle带进可取消栈。顶任务panic/abort后session仍能poll原子handle并汇总有界错误。flow token只在全部角色实际join后归还，不随socket rearm/lease归还/顶任务结束释放。新DATA行为、持久custody与stop不是纯Move。

每节点TuicUpstream lifecycle独占current/candidate/predecessor、合并恢复、任务和线性drain witness。`take_drain_handle`在首次await前取得；`drained(self)`开始后以pinned future保留在owner，超时/取消只结束观察。`into_0rtt`失败返回的Connecting继续同一attempt；真实失败candidate结算前禁止新attempt，accepted future也必须有owner。endpoint rebind不改变connection incarnation；slot0才是UDP来源，aux替换只动aux。Authenticate无成功ACK：t_ready仅为TLS与本地auth写/准入完成，t_tcp/t_udp必须各有真实往返。

### B. N1 必要预算与超限处置

下列为**未来实现的验收上限，不是当前代码保证**；每次接入以实际计数/独立求和/确定性tracer验证。F=4096生命周期flow tokens、W=128完整open custody、U=256 UDP uni readers、N=1..16默认2、C=N+3≤19。

| 资源 / 唯一 owner | N1上限与动作 |
| --- | --- |
| startup / TUIC | 全局90s；单握手5s；primary1次，aux各最多3次、250/500ms间隔；全节点同时物理握手1。N1 acceptance要求实际N个slot均建立，不以降级小池当pool覆盖 |
| recovery / TUIC | 从首个需求开始15s绝对deadline，最多2attempt、至少500ms间隔；每slot一个合并标记、全节点最多16。reservation/mutex/握手都计时；新等待者不重置事务/退避；失败candidate未drain仍占槽，第二attempt可能因无容量而明确失败 |
| 长期再探测 / TUIC | 500ms指数退避至30s，成功复位；认证/配置拒绝停该配置自动重试；不把open×recovery×background乘法放大，不另spawn timer/drainer |
| open / session | 20s总deadline含排队/恢复，Connect阶段≤5s及remaining budget；Connect仅提交一次，结果未知不重放。W覆盖等待/worker/结果/late cleanup；提取payload/spawn前取permit；每流256KiB、等待总32MiB，满则原socket背压 |
| connection / TUIC | N current＋candidate1＋primary predecessor1＋aux predecessor1。失败/closed tail继续占原槽直到真实drain，无额外尾队列。健康前代TCP正常维护时不强杀；仅明确故障或显式shutdown结束其连接 |
| UDP assoc / loop | 活跃1024，idle60s/sweep1s；实际primary generation内wire ID1..65535不复用，61440开始轮换、余4095。predecessor槽占满时保留健康工作及旧关联，耗尽仅拒新关联；物理归还后可轮换，不是进程寿命quota |
| UDP identity / 原生产者及AssocTable | producer绑定upstream/实际slot0 incarnation，本地association incarnation贯穿映射和回调。新primary发布时退役旧UDP并清旧partial；不在出队时贴当前代、不存65535个完整tombstone；aux故障不重置UDP |
| UDP downlink / TUIC | channel及待发布payload合计1024packet/64MiB；每encoded≤66KiB；reader总U×66KiB另计16.5MiB，读取5s deadline；queue TTL1s。reserve先于copy/publish，过期/超限明确计数丢弃，无恢复补发 |
| reassembly / 原reassembler | 256partial×65507B=16,769,792B，每partial≤255fragment slots、TTL10s，完成另有≤65507B输出；source incarnation入key、退役即清。payload不是metadata/RSS上限 |
| UDP uplink / udp_send | 保留256custody slots/scan4/一次poll/FIN或RESET；stop先拒新准入、原槽结算；不逐包spawn |
| DATA与control / 原loop | tick5ms、uplink64、remote16chunks/512KiB、flush默认256KiB、D16每flow512KiB/全局64MiB不变；control每turn≤32项、≤1connect后yield，无逐包全库存。公平性通过受控调度推进DNS/UDP/无关TCP证明，真实时延待测 |
| shutdown / session及节点owner | 15s观察时限；writer-stop5s、ADR14 pending-write90s/half-close10s仍按原用途。未完成返回Incomplete且原future/permit/首错保留；main继续cleanup，不能drop runtime伪造clean |

**task总量**：每flow supervisor/reader/writer/可选dispatcher/可选ordered-pump≤5；open W；uni U；每connection stats＋Quinn driver≤2C；固定8为session/main、TUNpump、UDPpump、endpointmonitor、endpointstats、sendstats、可选failoverhealth、Quinnendpointdriver。总`5F+W+U+2C+8=20,910`。恢复、0-RTT、drain、observer不另spawn；四处pending self-wake改reader-owned Sleep。所有stats/health/monitor登记并join；增强ResumableActionBridge不属N1兼容flow，其义务原样保留。

**同时保留timer实例总量**：每flow6（close guard/live diagnostic/read probe/self-wake/writer shutdown/child settlement）；每open3（总deadline/互斥phase deadline/liveness）；每uni1；每conn2（stats/Quinn driver）；固定20（原loop8、UDP heartbeat/stats2、monitor/endpointstats/sendstats/health4、startup1、recovery deadline/attempt/backoff3、shutdown1、queueTTL sweep1）。总`6F+3W+U+2C+20=25,274`。association/reassembly用条目timestamp和共享sweep，不逐条timer。该模型须与实现的全部reachable spawn/Sleep逐项对应；外部runtime/内核/allocator额外占用独立标注，不作整进程RSS证明。

**D5-B retained error**：单条RetainedFailure的文本≤1024B，含header/padding的`size_of`≤1152B，编译断言；固定class/phase/resource及attempt ID/OS及可用协议code/truncated/opaque/已知原长。已知文本直接有界取UTF-8前缀；io保留kind/raw_os_error，FromUtf8保留位置；未知Display/Debug/Any不格式化、不clone/存原对象，panic记Panic/Opaque与角色来源后释放payload。

每任务/物理角色槽一条首错（任务已结束但其资源未settle仍占该角色槽）、session sticky首错1、完成ring64，总20975槽、record body≤24,163,200B。inline握手/连接错误归其candidate/connection driver物理槽，startup/同步拒绝归session槽，不另留无计数error对象。子结果/通知只携FailureId；转交/ring入队move同一记录，未结算首错不被ring覆盖；额外错误及ring eviction用饱和计数显式记录。未知JoinError/payload不能藏在“已完成未消费”结果旁路，必须在受控边界及时转换；此上限不约束外部runtime瞬时分配/RSS。若实际错误来源无法对应这些槽，视为真实预算缺口，禁止加无界旁路。

### C. 精确未来 write scope、纯Move 与增长停止线

仅在N0接受且另获N1授权后启用以下范围。不得编辑third_party/Cargo依赖、P3行为或新增allowed-file之外owner；单Writer拥有重叠module，Integrator独占治理ratchet。

| 既有文件 | 允许轴/符号 |
| --- | --- |
| src/main.rs | main持session与真实stop/非零结果；tuic_tcp_sink_probe构造/结算；原2tests外置 |
| src/device.rs | VirtualTunBackend::Pumped、spawn/run_tun_ingress_pump、new_pumped、Drop、TunIo stop/join及真实impl；read/reserve均可唤醒 |
| src/client_tun.rs | startup composition、run_event_loop/run_owned_event_loop持久drive与退出；HandshakeDone/publish、handle_local_payload/open custody、SpawnedRemoteRelay/spawn_remote_relay、四类supervisor/writer shutdown结算、UDP来源与关联入口 |
| src/tuic.rs | TuicUpstream构造/handshake/0RTT/live/reconnect/pool replacement/start_udp/stats/monitor/Drop；reader/wake/pump与实际slot身份；AssocTable（3672/3690）的wire分配/轮换/retire与fake-IP归还 |
| src/tuic/udp_send.rs、udp_reassembly.rs | 停准入/原custody retirement；producer-bound source隔离/有限退役 |
| src/udp_relay.rs | 仅真实FourTuple/FlowEntry（23/30）的必要身份字段适配；AssocTable实现仍在tuic.rs，不创建第二份表 |
| src/owned_upstream/udp.rs、legacy.rs | LegacyRaw来源metadata与桥接；不改增强协议语义或扩大生产raw capability |
| src/failover.rs | health task登记/stop/join与必要生命周期调用；N3前置的TUIC Native/D16 variant缺口仍保留，不在N1实现新选路 |
| src/shared/errors.rs | RetainedFailure私有实现seam；不全仓改写无关错误 |
| src/upstream.rs（N1-M之后用户批准S1） | 仅NativeTcpRelayStream的opaque、不可克隆、尚未spawn/poll的prepared pump控制载荷及必要构造/拆分seam；接受公开struct构造方式变化，保持三个上游trait方法签名 |
| src/harness.rs、tuic_direct_probe.rs | 10处run_event_loop/2处run_owned_event_loop及run_cross_host_direct_tuic_probe_from_env实际owner适配；不运行外部probe或改变原测试目标 |
| 既有相关私有test索引/调用点、tests/code_growth_governance.rs与repository provenance既有inventory | 仅匹配上述移动/签名与shrink-only inventory；治理更新只归Integrator |

新生产文件仅4个，均≤1500行：`src/client_tun/compatibility_runtime.rs`拥有会话控制，`src/tuic/lifecycle.rs`拥有节点生命周期，`src/tuic/tcp_readers.rs`拥有完整native reader实现，`src/shared/errors/retained.rs`拥有有界错误值。不是4套协调authority；原DATA loop/relay engine留原父模块，不拆完整settlement来凑行数。

N0接受后的S1补充：用户在2026-09-18明确回复“允许”，仅批准上表upstream.rs载荷seam及既有8处构造适配；详见N1执行记录S1和s1-approval.json。TUIC只prepare；session完成可失败准备并预留flow/角色后，同步spawn→登记唯一handle→发布。Drop只stop，读与满channel均可stop，pump lease保持到实际结束；持久custody收集join/有界首错。仍计入既有五角色与W/F存储，不加registry/helper task/timer/error槽。原N0审查/hash保留为该时点证据；此段是后续明确批准的修订，不追溯冒称原卡已包含。

| 纯Move步骤 | 原符号/范围 → 目标 | 原characterization及边界 |
| --- | --- | --- |
| M0a | device.rs631–812共182行inline tests → src/device/tests/mod.rs | 原8项名字不变，后续新增stop tests在private lifecycle叶，不能塞入生产文件 |
| M0b | main.rs161–192共32行 → src/main/tests/mod.rs | 原2项；crate root显式#[path]避免src/tests冲突 |
| M1 | tuic.rs2411–2483、2537–3480共1017行 → tcp_readers.rs | 完整Tracked/native reader slice；2485–2535两个既有helper保留；原12项见下 |
| M2 | client_tun.rs7956–8291共336行run_event_loop_sinked/start_tun_proxy → compatibility_runtime.rs | upstream_kind_selector＋tun_runtime_config_*共6项，只锁配置/选路，不冒充真实startup覆盖 |

M1原12项均为`tuic::tests::`：d16_direct_ordered_reader_forwards_exact_max_len；d16_direct_ordered_reader_keeps_one_pending_read_future；d16_direct_ordered_reader_never_returns_more_than_max_len；d16_direct_ordered_reader_rejects_offset_discontinuity；d16_real_quinn_reader_sustains_ordered_progress；d16_real_quinn_reader_wakes_after_delayed_ordered_payload；native_ordered_chunk_batch_combines_contiguous_bytes_and_advances_offset；native_ordered_pump_holds_pending_read_until_ready；native_ordered_pump_reader_batches_ready_chunks_to_max_len；tracked_relay_stream_records_nonempty_reads；tuic_stream_pending_self_wake_arms_for_active_connection_rx_after_deadline；tuic_stream_pending_self_wake_services_active_connection_rx。baseline-05仅枚举过这些名字，不代表12项已全执行。

M0/M1/M2逐步原characterization GREEN→纯Move→GREEN，默认/all编译和growth/repository provenance通过；其后才N1行为。tuic7991/client_tun16561等旧超大文件保持shrink-only，缩短同一步由Integrator降inventory。新文件接近限行先估算；超过1500、新allow、再分割同一settlement、出现表外owner即停止该步骤，报告最小scope修订，禁止净零行/额外sidecar/新grandfather绕过。

新private test路径限于：`src/device/tests/{mod,lifecycle}.rs`、`src/main/tests/mod.rs`、`src/shared/errors/retained/tests/mod.rs`、`src/tuic/lifecycle/tests/{mod,recovery,settlement,udp_identity}.rs`、`src/client_tun/tests/compatibility_lifecycle.rs`及`compatibility_relay_custody.rs`；叶≤800/index≤300。现有tuic/tests/mod.rs、client_tun/tests/mod.rs及其被改签名的调用点只做必要适配且不增加旧债；若实际无法在此scope闭合必须停并报告，不能顺手扩P3。

N0接受后的S2补充：用户在2026-09-19明确回复“确认”，批准`src/client_tun/tests/compatibility_open_custody.rs`及`src/client_tun/tests/compatibility_flow_custody.rs`两条私有测试叶（各≤800），通过现有custody测试模块的private外置child接入；Integrator扩原syn来源门禁并保留原341及custody10测试身份。仅测试路径范围修订，生产owner/API/文件范围、任务/timer/error预算和所有门禁不变。授权绑定提案见`../current/n1-20260918-evidence/s2-approval.json`；原N0审查为历史证据，不追溯冒称原卡已包含。

N0接受后的S3补充：用户在2026-09-19明确回复“确认”，条件批准第五个新production实现路径`src/client_tun/compatibility_startup.rs`（仍≤1500）。仅将既有336行`run_event_loop_sinked`/`start_tun_proxy`完整启动壳和专用imports作行为保持移动；runtime私有path child加单entry reexport，父文件不增行，session/custody/testseam/FQN保持原地。必须先R2及当前W边界必要测试/review/growthGREEN，再实际重算容量，随后纯Move绿到绿，再F/S1行为。没有新增owner/task/timer/error预算或提高限额；最终冷session先于任何async初始化的N1义务仍在。详见`../current/n1-20260918-evidence/s3-approval.json`，绑定原proposal。

### D. 首测、完整处置与有限执行次序

N1授权后的顺序：M0/M1/M2分别GREEN→Move→GREEN；N1-L实现bounded error、device双阻塞stop、稳定session与open/per-flow custody；N1-R实现同attempt/真实drain/恢复与UDP代际；完成main/probe/harness真实cutover后做真实peer整矩阵。每步可编译，半套owner不得启用生产入口；N1-R和N1-L一起接受才是N1完成。现有错误和P3义务不因接口适配而消失。

第一业务tracer路径为`src/tuic/lifecycle/tests/recovery.rs`，全限定名`tuic::lifecycle::tests::recovery::single_upstream_explicit_loss_recovers_tcp_and_same_udp_flow`。未来命令为`cargo test --offline --locked --lib tuic::lifecycle::tests::recovery::single_upstream_explicit_loss_recovers_tcp_and_same_udp_flow -- --exact --test-threads=1`，随后加`--all-features`复跑。**当前文件/测试不存在，不得执行0项当GREEN。** 首RED应是实际恢复/代际/结算断言，fixture/import错误不是业务RED。

测试使用固定sing-box1.14.1与已验hash、受控临时CA/leaf、loopback TCP/UDP echo targets；正式生产owner发起真实TUIC，测试只close其实际current Quinn connection，peer/targets保持存活，不安装fake成功对象或export raw控制API。首次node测试之外，必须由同一实际session+现有内存TunIo harness证明公开loop到owner可达；新call-path断言置compatibility_lifecycle叶，不用真TUN。

profile：native UDP、Cubic、默认MTU1280+PLPMTUD、ALPN h3、0-RTT关闭（另有开启/拒绝确定性分支），零人工loss/delay。pool1/2/16各10轮，必须核对实际slot数；每轮32并发新TCP、16固定本地四元组UDP，UDP每流512B/20ms，TCP每流双向内容校验至少64KiB；业务观察20s/退出15s。每轮故障前后均记录真实TCP/UDP往返与无关健康流；32/32新TCP、16/16 UDP恢复、零串话/健康流误杀/正常阶段错误。UDP故障期丢包计数保留，不以恢复补发掩盖；实际RTT/MTU、CPU/RSS、队列/task/connection/timer峰值及调度间隔全部记录，未测性能Unknown。

单调时钟t_fault/t_detect/t_ready/t_tcp/t_udp分开；全部30轮/尝试入样本，超时作为失败及删失数据披露；报告P50/P95/P99/max但不以30样本推导尾部概率。20s是该本地profile接受deadline，不是3s/5s SLO或WAN/长时结论。正常段与故障后恢复的payload序号/内容均校验。

| 分支 | 必须随N1锁定的实际断言 |
| --- | --- |
| success / isolation | 新primary UDP真正往返＋健康aux TCP继续；aux故障不影响UDP/其他slot；ID轮换不杀旧健康TCP，拒旧代包/片段/late callback |
| rejection | 配置/认证/容量失败不发布、无业务未知重放；Authenticate本地发送不当远端接受证据 |
| pressure / concurrent | W/F/U/C及各byte cap达界，无额外task/handshake；原TCP payload仍归原owner，同资源恢复合并 |
| cancellation | startup/open/relay/shutdown观察者取消后重入同一owner；drain future/错误/permit没有drop或复制；blocked-read与blocked-reserve TUN pump均stop后join |
| failure | 每类relay engine的reader/writer/dispatcher/pump真实错误与panic均不被顶成功覆盖；bounded error文本/opaque/truncated/首错/ring move计数正确 |
| shutdown | 恢复中/候选失败/已替代各处stop；真实connection/endpoint driver drain；timeout仍非零且同future继续，全部clone/任务实际归还后才clean/FailedSettled |
| 原路径回归 | DNS/fake-IP/UDP/idle/pending-write/D16/close-tail及原增强provenance均保留；原device consumer-close测试不能替代blocked-read stop tracer |

本地baseline复用本结果所有精确JSON命令与非零计数；新版本重新hash/复跑受影响门禁，两模式lib/bin编译、growth7、repository20、Knife provenance220及相关call-path。每个新完整处置tracer必须锁定非零exact数量；不借full suite总数替代目标行为。repair-test失败先分析证据/给具体方案并等用户确认；禁止随机第二次修补、删除WIP、扩大权限或开始外部验收。

## 当前可复制交接

N0已完成；当前停止在单独N1授权之前。唯一目录 `/Users/liushan/Documents/Personal/Languages/Rust/mini_vpn-recovered-20260918-jIMbzz`。先读AGENTS、Rules、STATUS顶部、指南§4/5/13、本结果同一N1卡、authority.json/review.json。N0-A及最终N0-B/C均独立Approve0/0/0；用户D1–D4、D5-B、F1-R1均明确批准。F1-R1的8命令134次通过及真实peer loopback准备独立接受；加原388和Move基线20，共542次通过，原缺钥/CaUsedAsEndEntity各1次失败保留。公开cert恢复/临时身份删除。449Rust、HEAD与978stage entries不变，raw index仅已记录cert stat缓存变化。N1卡的资源/task/timer/error数字是未来验收上限，不是已实现能力或RSS证明。

下一步必须先有用户单独N1授权；当前Rust allowed-files为空，勿因N0完成自动开工。授权后以本卡M0/M1/M2原characterization GREEN→Move→GREEN开始，再按卡实现N1-L/R并完整验收；凡新repair-test失败先分析最小方案并等确认。root此前唯一N0 Writer，其他agents只读；后续须先核对Writer。P3 aggregateP1、五个51B、Case C/R7/Task4及旧M2全部保留。仍禁止P3行为、commit/push、外部/TUN/VPS/WAN/M2与完成通知；原库/recovery只读，不reset/clean/stash，不新建同义架构sidecar。

## N0 最终出口验收 — AcceptedLocal

wip_audit复核有限scope定位修正后给出P0/P1/P2=0/0/0、Approve，明确全部N0出口齐备；Integrator据此记录完成。原4项NotReady在设计层面闭合；后续唯一P2为AssocTable文件定位错误，已修正而未改Rust。原审查/失败不删除，review.json记录接受版本与阶段限制。

| 出口 | 真正证据 |
| --- | --- |
| 可恢复现场 | 1270文件归档逐成员读回、449Rust匹配；checkpoint_review和N0-A独立Approve |
| 契约取舍 | 用户D1–D4、D5-B明确回复；指南§13与authority.json |
| 调用链、owner、预算、精确N1卡 | 本卡最终whole-scope Approve；真实入口/资源、完整处置、Move与增长stop已定位，尚无N1代码 |
| 必要本地基线与差异分类 | default/all lib与bin编译；累计542次测试执行通过；2次历史环境失败保留；F1-R1和真实peer配置/监听/退出准备接受 |
| 最小交接演练 | handoff-review.json恢复权限/唯一下一步；blocked记录不洗白、缺证据卡被拒绝；最终导航已同步 |
| 独立审查与范围 | review.json最终0/0/0；449Rust不变、978stage entries相同、原cert恢复、临时key删除、无遗留cargo/rustc/sing-box进程 |

本轮实际改动为6份既有文档、1份N0结果及检查点/测试/审查证据；仅在target内准备固定peer和临时测试身份，原身份已恢复。没有新Rust行为、N1恢复业务、生产shutdown或性能验收，没有commit/push或外部运行。N1首次完整路径、N2–N5以及全部保留增强义务仍待未来各自授权/实现/验收。


N0接受后的S4补充：用户在2026-09-19明确回复“确认”，批准`src/client_tun/tests/compatibility_open_settlement.rs`及条件性`src/client_tun/tests/compatibility_flow_settlement.rs`，均为原open/flow测试父的private外置child且各≤800。原3个open FQN和341+10身份保留；F child仅在fresh实际fixture/完整矩阵预算不能容纳于原800行叶时使用，同一fixture不分散生产authority。S3前置为pre-F W局部完整控制证据，真实W→F、Native latepump/lease join及健康已安装TCP公平性仍属之后F/S1的N1必需门。原production范围/1500限额与所有预算不变；S4不豁免1302–1542生产估算上界风险。批准绑定`../current/n1-20260918-evidence/s4-approval.json`，原未授权提案历史保持。


N1授权后的S5补充：用户已明确“确认”，批准一个额外private测试路径`src/client_tun/tests/compatibility_open_pressure.rs`（≤800），作为原open_custody父的private外置child，共用唯一真实fixture，原7个open FQN/body保持。压力叶承载完整bytes/追加deadline、FIN/reap/stale、DNS/UDP/32控制项-1Connect/cheaptrue及只读observer；原settlement继续成功/非法库存/pollDrop，所有生产scope/1500与预算不变。绑定原提案f5e48fd5e及独立0/0/0的实际授权见`../current/n1-20260918-evidence/s5-approval.json`。S3前置、F/S1与节点/cold-start义务仍在，提案时生产1376–1626估算上界风险未获豁免；后续runtime1275时点把cold补齐后的1454–1754估算见l1e-f-cold-capacity-audit.json，须在实际W/S3后再次按完整F+cold关闭容量门。

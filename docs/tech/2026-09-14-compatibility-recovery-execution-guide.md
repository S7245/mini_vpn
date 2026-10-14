# 标准 TUIC 兼容路径恢复：N0—N5 执行指导

日期：2026-09-14。版本：1。类型：转轨与研发执行指导。

**2026-10-09 用户范围修订：N3 自动节点切换移出本轮必做范围，进入后续待办。** 节点启动失败由用户自行选择其他节点；自动启动备用、运行中自动换出口和自动回切均归延期 N3。当前规划顺序为 N1 → N2 → N4 → N5，N4 不再依赖 N3；后续阶段仍须各自授权。N1/N2 对用户选定节点的自动重连、故障识别、有界重试与资源回收义务保留。用户同时确认：恢复新 TCP 与后续 UDP 通信，不保证原应用连接无缝续传。第13节原批准记录保留为历史；涉及 N3 的旧范围以本修订为准。

**2026-09-18：用户已授权执行 N0；N0 尚未通过，N1 Rust 未授权。** 当前唯一工作目录为恢复副本 `mini_vpn-recovered-20260918-jIMbzz`。本次授权与取舍候选见第 13 节，实际检查点、基线、阻塞和 N1 准备卡见 [N0 结果](2026-09-18-compatibility-recovery-n0-results.md)。原创建时本文只是一份指南；该历史不等于本次通过。本文不授予外部验收、commit/push 或后台定时任务权限。

正式使用时，由获授权的主任务 Integrator 完成 N0，记录适用条款的批准与独立审查，并把新的当前阶段接入 STATUS。此前仍以现行规则和实际主任务状态为准。本文后续使用“必须”描述拟采用路线的验收要求，不表示这些能力已经存在。

## 1. 快速使用方法

1. 冷启动只读 [AGENTS](../../AGENTS.md)、[产品要求](../../Rules.md)、[当前状态](../current/STATUS.md)、本文及当前结果的必要段落；不加载全部历史。
2. 未获 N0 授权：仅准备取舍和请求；获主任务授权后执行第 4 节，不开始 N1 Rust 实现。
3. 已激活：在第 5 节唯一剩余工作清单中选择最早的未完成、依赖已满足项，为它填写第 10 节执行卡。
4. 一个 Writer 完成一个完整行为切片，经门禁和独立审查后，由 Integrator 更新证据与当前状态；按第 9 节决定继续或停止。
5. 本文保留稳定契约、清单与模板；详细命令输出进入阶段结果，不把本文变成日志。

**下一步不是先完成整个 P3/Case C/R7，而是获授权后完成 N0：取舍、检查点、真实调用链和 N1 执行卡。** 若现有 WIP 影响共同编译或兼容路径，先定位具体依赖；不得为存档而无限扩展增强路线。

2026-10-13 用户已批准并经独立技术审查的[分层恢复修订](../current/n1-session-rebuild-contract-20261013.md)仅覆盖会话级故障与 N1 实施顺序的冲突条款。持续服务 owner 不变；局部故障保留会话，明确会话级故障可在旧资源已结算后受控替换一次会话。当前 N1 的唯一可执行细目在[F1 清单](../current/n1-f1-implementation.md#single-remaining-n1-implementation-checklist)；本节以下历史 evidence 不代表新能力已实现。

## 2. 交付契约与非目标

### 2.1 近期产品承诺

在用户选定的 Upstream 可重新连接且具备所需能力时，同一客户端进程能在指定故障后恢复新的有效 TCP/UDP 通信，无需用户重启 VPN。该节点启动失败时明确报告失败，由用户自行选择其他节点；本轮不承诺自动跨节点恢复。保留 TUN、fake-IP DNS、标准 TUIC v5 互通及高并发、吞吐、长时质量要求。

| 对象 | 必须做到 | 不承诺 |
| --- | --- | --- |
| 健康既有 TCP | 局部故障继续隔离，选路变化不关闭它；仅修订契约定义的会话级故障允许结束该会话内全部 TCP | 为低延迟迁移或将普通 Target/空闲/业务超时升级为整体重建 |
| 已失效 TCP | 按已定义的错误/关闭处置有界结束，使应用可观察失败 | 跨新连接继续原 Target 字节流 |
| 新 TCP | 在建连预算内选用可用出口；失败有明确原因 | 透明重放已发送或结果未知的应用数据 |
| 持续 UDP 流 | 即使本地四元组不变，后续新报文也能建立替代出口关联 | 保留原出口 NAT 映射、保证应用会话不中断 |
| 旧 UDP 工作 | 隔离迟到报文、旧分片与旧回调；显式处理过期/丢弃 | 跨出口可靠重放旧报文 |
| 全部出口不可用 | 有界等待/拒绝/丢弃、可观察、不泄漏到未授权直连 | 不存在可用路径时仍联网成功 |

VPN 的“路径恢复”、应用的“自动重连”和视频的“恢复播放”分别验收，不互相替代。UDP DATAGRAM 不因使用 QUIC 就获得可靠重传，参见 [RFC 9221](https://www.rfc-editor.org/rfc/rfc9221.html#section-5)。

### 2.2 N0 应冻结的建议范围

- 本轮保留单个用户选定 Upstream 的恢复；两个成熟 TUIC Upstream（A/B）自动切换按2026-10-09用户决定延期至后续 N3。
- 现有 REALITY TCP 备路可保留，但不得把当前 TCP-only 实现标成 UDP 备用能力。
- 建议本轮不承诺“所在网络完全封锁 UDP/QUIC 后仍恢复 UDP”。如用户要求，先明确成熟可用的 UDP 承载、互通与性能代价，重估范围；不能默默沿用原工期。
- 保留增强路线及其未完成义务：同一 Target socket、应用 ACK、跨链路重放/去重、自研 owner-server、Case C/R7 及相关后续能力；暂停新增投入须由主任务明确落实。
- 不包含 GUI/App、后台控制面、四平台发布、自研新 UDP wire protocol、整体更换数据面引擎。
- 不批量撤回 Quinn/smoltcp 本地补丁，不反向合并已接受的 Module 拆分，不删除无关 WIP。

这组建议不能覆盖现有接受契约。N0 必须指出 [ADR-0011](../adr/0011-tuic-reality-auto-failover.md) 中 UDP 固定 TUIC、检测与重试条款的变化，以及 [ADR-0015](../adr/0015-path-diverse-resumable-upstream.md) 中兼容/增强验收的适用范围。[ADR-0014](../adr/0014-established-relay-lifecycle.md) 的健康空闲 TCP 保护继续有效。原 Knife15/M2 失败记录保持不变。

## 3. 技术落点与不可破坏的约束

### 3.1 从实际代码开始，不预先发明新 Root 类型

下表是起点导航，不是 Rust allowed-files；执行前必须重新定位符号和调用者。

| 现有位置 | 需要核对的 Seam | 对新路线的意义 |
| --- | --- | --- |
| [client_tun.rs](../../src/client_tun.rs) | Upstream 构造、LegacyOwnedUpstream、UDP 下行与事件循环接线 | 证明实际公开路径会经过新 Implementation |
| [tuic.rs](../../src/tuic.rs) | reconnect_locked、start_udp、pool generation replacement、Drop | 收敛生命周期，不重复启动泵或争抢替换权 |
| [upstream.rs](../../src/upstream.rs)、[failover.rs](../../src/failover.rs) | open_tcp_relay、open_tcp、open_is_cheap、send_udp | 避免选路时丢失 Native/D16 或阻塞事件循环 |
| [owned_upstream/udp.rs](../../src/owned_upstream/udp.rs) | UdpDownlinkSource、关联与重组接线 | 在生产者处绑定真实来源并贯穿到消费 |
| [Cargo.toml](../../Cargo.toml) | 本地 patch 与构建 feature | 复用已有修复，固定 source/binary 对应关系 |

2026-09-14 只读核对发现：failover 没有覆盖 open_tcp_relay，默认实现会返回 Generic；其 UDP 固定转发 TUIC；start_udp 返回 Receiver 并启动后台任务，不能仅凭 Drop 推导完整 join。它们是审计入口，不是完整因果审查或已通过测试的结论。

### 3.2 拟采用的职责划分

- **选路 Module**：按 TCP/UDP 能力、故障证据、冷却和预算决定新准入；不直接关闭健康既有流，不直接持有和操纵另一 Module 的后台任务。
- **每个 Upstream 的生命周期 Module**：拥有其连接建立/认证、代际、相关泵/探测/重连任务、取消及完整回收结果。内部可分 private child，外部 Interface 不要求调用者拼接清理顺序。
- **流/关联归属**：TCP 绑定原 generation；UDP 本地流与出口关联分开。池中某个 slot 的替换不等于整个 Upstream 或 UDP generation 更新。
- **数据转发**：保留既有字节所有权与背压路径。选 TUIC 时按配置保留 Native/NativeByteOwned；REALITY 可使用其实际支持的 Generic。禁止默默退化，也不强造跨 Transport 的同一种引擎。

删除某个新增 Module 后，若其复杂性只是消失而不回到多个调用者，它很可能是浅 wrapper；应重新检查必要性。不要把策略、所有连接和 DATA 热路径塞进单一全局 Root。

### 3.3 必须在实现中证明

1. **唯一恢复权**：同一资源的并发恢复请求合并到一个有界事务；Endpoint recovery、pool replacement、健康检测各自的职责和仲裁明确。
2. **提交与所有权**：每个生命周期事务在发布可用性前完成其必要的 fallible preflight；失败精确返回或保留原资源，未知结果保留非零失败处置。不是声称发布以后不会有网络故障。
3. **取消与关闭**：观察 future 被取消，不丢失资源或任务的 owner；stop 后禁止新准入，驱动已拥有的关闭/回收，保留真实错误。不得以丢 handle、忽略结果、泄漏或 abort 即干净代替证明。
4. **本地 UDP 身份**：Upstream identity、实际连接/slot incarnation、association incarnation 以及重组 packet identity 有明确作用域；wire assoc-id 不是全局唯一身份。
5. **身份绑定时机**：来源由实际接收任务绑定并携带到消费；禁止出队时贴“当前 generation”。association ID 复用、迟到分片和旧清理回调均不得命中新关联。
6. **恢复不等于重放**：尚未提交的建连准入可按契约重选；已提交或结果未知的业务数据不能因为超时自动重发到新 Target。过期 UDP 不以恢复为由排队补发。
7. **故障隔离**：Target 失败、连接失败、整个 Upstream 失败分别记证据。认证/配置错误不进入无限瞬时重试。
8. **热路径**：不新增每包全库存扫描、全局锁或仅为治理产生的编码/分配；握手/恢复等待不能饿死 DNS/TUN 与无关流。
9. **安全与配置**：不降低认证校验；恢复失败不擅自直连；Upstream 域名的 bootstrap DNS/路由不能循环依赖已失效隧道。测试使用受控身份，不记录凭据。

上述约束只覆盖本轮实际拥有的资源树，不导入增强模式的全部协议和 ledger；共享生命周期缺口若真实阻塞本轮，仍必须修复。

### 3.4 代码增长与 AI 导航

沿用 AGENTS 的可执行门禁：新 production 文件≤1500 行、test leaf≤800 行、test index≤300 行；旧债精确 shrink-only。Writer 先定位符号/调用者/owner，再按需要把完整 Implementation 放入有 Depth 的 private child；不以净零换行掩盖 Interface 扩散。纯 Move 与行为修改分开。oversized 缩短由 Integrator 调低 inventory，达到正常上限时移除条目；新路径不得加入 grandfather 清单，不新增参数数量豁免。

新 Module 的短文档说明拥有/不拥有的资源、不变量、入口与测试位置。使用符号搜索和局部读取，不把完整大文件、历史日志或多个重复架构叙述塞入冷启动上下文。

## 4. N0：有限设计、准确保存、可以开工

N0 建议投入 1—2 个有效研发日作为复盘预算，而非到点必须通过。只有当前授权的主任务能够执行下面的切换动作；本文件的生成不执行它们。

### N0-A：停止交叉写入，保存现场

- Integrator 核实当前 Writer/命令真实停止，再记录 branch、HEAD、tracked diff、untracked 文件和当前失败现场；红灯状态也可以保存。
- 保存已接受、局部 GREEN 未接受、预期 RED、意外失败四类状态。结果带源码快照标识与命令证据；不能只记录 commit 而遗漏脏工作区。
- 使用现有证据工具和批准的持久化位置保存恢复所需内容；/tmp 可作运行目录，不能成为唯一长期副本。不要归档凭据或无关个人文件。
- 只做保留性保存。commit、分支/worktree 创建、文件回退或移走 WIP 均按实际授权执行，不能用 reset/clean/stash 隐藏问题。
- 分析 WIP 对共同编译与兼容路径的真实影响；无关增强缺口记录保留，不强迫先闭合整个 P3。

### N0-B：一次性冻结关键取舍

在本文或一份指定的正式范围修订中记录批准依据，避免多份同义契约：

- 第 2 节的兼容/增强承诺、UDP 全封锁范围、平台和两个 Upstream 首轮规模。
- 适用 ADR 条款及新的兼容模式验收名称，不改写原 M2 结论。
- N1 真实入口→TUIC→Target→返回路径、唯一资源 owner、复用/替换/保留项。
- 第 6 节所有 N1 所需的预算数值与超限处置；未测得的性能指标标为 Unknown，不能填成通过。
- 第 7 节恢复计时口径、工作负载、故障模型、所需真实 peer 版本与本地验证方法。
- 第 10 节 N1 执行卡：精确文件/符号、已有基线、预期失败、必要门禁、独立 Reviewer 和停止规则。

不需要在 N0 设计完 N2—N5 的所有类型。后续阶段只细化其新增资源与行为；不可把已知 N1 必需的清理、基本重试约束或代际隔离延期。

### N0-C：验收与出口

N0 完成需同时满足：可恢复检查点可读；契约取舍获批准；独立审查无阻塞项；N1 调用链和执行卡可执行；必要本地基线可复现且差异已分类；第 11 节最小交接演练完成。

若本地真实 peer 不可用，明确缺的二进制/配置/生命周期支持及获授权的准备动作，不能用 success stub 替换。N0 预算耗尽仍没有 N1 可编译路径时，提交一次整体取舍复盘，说明 blocker、选项和增量成本；不得靠反复增加 sidecar 延期。

## 5. 唯一剩余工作清单

激活后由 Integrator 更新此表的状态和证据引用；STATUS/HANDOFF/TODO 只导航到它，不复制另一套阶段清单。P=首条完整路径必要前置，V=本目标必做功能切片，L=本目标必做后续验证，D=明确延期的功能或增强目标。全部初始为未执行，不能从旧路线测试数量折算完成比例。

| ID / 类别 | 工作与阻塞原因 | 依赖 | 完成证据 | 当前状态 |
| --- | --- | --- | --- | --- |
| N0-A / P | 停写核实、准确检查点；保留非全绿现场 | 主任务激活授权 | [结果与检查点](2026-09-18-compatibility-recovery-n0-results.md)；1270 文件读回、449 Rust 不变、WIP/失败分类 | AcceptedLocal：独立完整记录 Approve 0/0/0 |
| N0-B / P | 冻结目标、owner、预算、首片；新旧承诺不能混用 | N0-A | 第13节D1–D4/D5-B批准；结果同一N1执行卡与F1-R1证据 | AcceptedLocal：最终独立Approve0/0/0 |
| N0-C / P | 基线与最小交接；确保可以恢复执行而不是靠聊天记忆 | N0-B | 结果中542次通过/2历史失败、交接与独立审查 | AcceptedLocal：N0完成；N1仍须单独授权 |
| N1-M / P（条件项） | 仅在现有 Seam 不足时做必要行为保持 Move | N0-C | 同一 characterization 前后 GREEN、growth/provenance 通过 | M0/M1/M2均独立AcceptedLocal0/0/0；1636次执行通过，仅接受纯Move，见N1记录 |
| N1-R / V | 所选节点局部重连及受控会话重建，恢复新 TCP/同四元组 UDP | 接入丢包修复、所用 Generic 资源树闭合；不等待全部 family/S1 | 第8节与分层修订§6真实恢复、代际、错误/清理及公开调用证据 | 未实现；按唯一 F1 清单推进 |
| N1-L / V | 本轮资源的压力、取消、退出与错误回收 | 与每个 N1-R 切片同时闭合 | 完整处置矩阵、真实join、无残留工作/虚假零、独立closure | Generic局部证据已接受，接入SynBurst仍RED；当前结果与余项见唯一F1清单，整体N1未接受 |
| N2 / V | 黑洞/慢速/空闲/Target 失败；防误判与所选节点重连风暴 | N1 完整接受 | 分层故障证据、基本策略深化、有界重试/公平性测试 | 未执行 |
| N3 / D | 自动启用备用节点、运行中 TCP/UDP 自动换出口及自动回切 | 未来单独授权；届时重新审查依赖 | 未来启用时仍需双向真实往返、同四元组 UDP、迟到隔离与回切迟滞验收 | 用户于2026-10-09移出本轮；见 TODO 后续规划 |
| N4 / L | 所选节点的本地并发、长时与循环故障组合验收；不含自动 A/B 切换 | N2 完整接受；不依赖延期 N3 | 完整本地矩阵；预算峰值/超限处置、任务收敛、DNS/TUN 公平性、同节点代际隔离与稳态质量 | 未执行；2026-10-09 用户确认修订范围 |
| N5 / L | 所选节点在深圳↔US真实链路上的兼容模式验收；不含自动备用/回切 | N4 完整接受；STATUS 明确 readiness；外部授权 | 固定配置下启动、同节点恢复、失败率、吞吐、UDP 丢包/抖动、长时资源与清理证据及最终审查 | 未执行；2026-10-09 用户确认修订范围 |
| E / D | 原会话连续性、owner-server、Case C/R7 与相关后续 | 单独重启增强目标的授权 | 原目标自身验收，不使用 N1—N5 替代 | 保留未完成 |

N1-R 不能单独冒充 N1 完成。N1/N2 每新增一种资源，相关失败/压力/取消/关闭测试随切片交付；未来重启 N3 时同样适用。N4 是本轮所选节点的组合验证，不补基本正确性，也不要求完成延期的自动切换矩阵。

N4/N5 精确范围（2026-10-09 用户确认）：

- **N4 保留**：所选节点上 TCP/UDP/DNS 并发与长时运行、重复明确断线和黑洞/慢速故障、恢复中的压力/取消/退出、健康流隔离，以及连接/任务/队列/内存峰值与最终收敛。重用 N1/N2 已通过的机制验证组合行为；负载、时长、故障次数和质量阈值按第7节在阶段开工前冻结。
- **N5 保留**：固定实际客户端、TUIC peer、source/binary/config，在深圳↔US真实网络测量启动成功率及首次业务可用时间、同节点恢复时间、新 TCP 与同四元组 UDP 后续包往返、吞吐、UDP 丢包/抖动、长时资源与最终清理。明确 RTT/MTU/方向/负载及全部失败样本；本地 GREEN 不替代真实网络证据，未测指标保持 Unknown。
- **移至延期 N3**：A 启动失败自动启用 B、运行中自动换出口、冷/热备用、自动回切与迟滞，以及这些自动行为的并发/反复切换、成功率和切换耗时矩阵。它们不阻塞本轮 N4/N5。
- **恢复边界保留**：同节点重连仍有新旧 generation；迟到 UDP/分片/回调不得污染新资源。所选节点持续不可用时有界失败，由用户自行选择其他节点；用户选定新节点后沿现有正常启动流程验证，不新增 GUI/App 或无缝迁移承诺。人工选择耗时单列，不计入程序自动恢复时间；应用自动重连/恢复播放与 VPN 路径恢复分别记录。
- **门禁保留**：N4 依赖完整 N2；N5 依赖完整 N4、STATUS readiness 与外部执行授权。吞吐目标仍须通过现有端到端容量/可达性门禁。本次仅批准范围修订，不启动 N4/N5 或外部验收，也不改变当前 N1 的代码、预算、接受位置和禁止项。


N1-L 历史执行细目（保留当时依赖与证据，不再驱动当前次序；已由唯一 F1 清单及2026-10-13分层修订覆盖）：

| 细目 / 类别 | 当前阻塞与依赖 | 完成证据 / 下一步 |
| --- | --- | --- |
| W准入 / P | R1有效RED；R2同exact1/1通过，独立复核，仅准入边界 | l1e-r2-gates/独立结果：原socket身份/bytes/window及一次失败worker处置通过；完整W仍见下一项 |
| W在F之前的局部控制 / P | R4局部接受保持；R5写前STOP仍保留。S6-MoveLayout proposal 已独立0/0/0，但等待用户对三条新路径的明确确认 | S6 只执行 fixture 与 event-lane 两个纯 Move：root 保留 select/Sleep/sender/生命周期，lane 只含 global_rx、handle-only D4、gap state/pause；不含 bridge/probe/行为修复。先 fixture green-to-green、再 event Move/相关门禁/独立review；之后另提 bridge/tracer 范围，才验证cheaptrue、两个FIN、stale/reap、双向库存、32控制项/1Connect及原DNS/UDP/其他pending TCP进度；不宣称完整W |
| S4测试容量 / P（已批准） | 既定测试路径无法容纳完整余项；提案仅新增私有W settlement子叶与条件F settlement子叶，各≤800 | l1e-s4-test-scope-proposal.json独立0/0/0：原3 FQN及341+10保留，复用同fixture；F完整估660–980，条件新叶须先实际预算。s4-approval.json已记录用户确认，继续实施 |
| S5压力测试容量 / P（已批准） | S5时实际open630/settlement518、余452是历史快照；R5当前为shared fixture parent/open-custody693/800（余107）、pressure409/800（余391）、settlement794/800。完整余矩阵估470–715，R5复核确认现有parent/pressure无可信闭合容量 | S5原路径授权和证据保留，但不允许压缩断言或把缺口伪装成同叶完成。S6-MoveLayout proposal/独立review已固定先释放 fixture 容量与 event owner seam；在其明确确认和绿到绿前不提前S3或豁免生产风险 |
| S3容量纯Move / P（已批准条件项） | 等上述pre-F W局部门GREEN并重算容量；不能要求依赖F的完整结算先完成 | 原336行启动壳逆变换、六characterization及W/device/治理绿到绿；custody/parent不扩。S5时runtime1275是历史快照；S6当前runtime1279且要求字节不变。S3开始前必须重新计量，再评估约929–939及余W+F+cold完整估1454–1754，见l1e-f-cold-capacity-audit.json；1500不变，若实际无法闭合仍停 |
| F/S1及完整W结算 / P | pre-F W及容量门；原Native pump尚未prepare/登记/真实join | 同root F4096、五角色、四family/Generic五分支完整处置；W→F成功/压力移交、Native latecleanup/lease实际join与健康已安装TCP公平进展均须实际测试后才可接受；不得以pending TCP或Drop/abort替代 |
| 节点owner与接入 / V | F/S1及N1-R真实恢复/UDP代际；半套owner不能启用 | coldsession先于async构造，main/probe/harness真实接入；N1-R/N1-L完整真实peer矩阵和独立接受 |

## 6. 预算与恢复策略

### 6.1 N0 必填预算表

从实际配置/实现上限和已接受证据提取；没有值就列为具体 blocker。每项填“数值、单位、作用域、owner、超限处置、证据”，不以“有界”二字代替数值。

| 资源 | 必须覆盖的总量 | 超限原则 |
| --- | --- | --- |
| TCP open/等待恢复 | 并发尝试数、等待流数、单流与总等待字节、总等待时间 | 拒绝或有界背压，不能无限 spawn/缓存 |
| 连接 | 每节点 active/candidate/draining 和节点总和、drain 时间 | 先预留再创建；保留既有 lease，不能丢健康 predecessor |
| UDP | 活跃关联、复用隔离窗口、包数/字节、分片与重组总量、TTL | 明确可计数丢弃；不跨身份混合 |
| 后台任务/探测 | 每 owner 和全局任务数、并行握手数、计时器、待合并请求 | 合并、限流、退避或拒绝 |
| 关闭 | 正常 drain 与 join 预算、失败证据的有界保留 | 超时为失败/未完成处置，不伪造 clean |
| 事件循环 | loop/batch/flush/队列预算与非数据工作份额 | 不让恢复饿死 DNS、TUN 或无关流 |

容量算式需避免漏算或重复计算。例如拥有的缓存上界可按各层独立持有量求和，连接数按各 Upstream 的 active+candidate+draining 求和；另列 Quinn、内核、allocator 的额外占用，不把自有队列账本称为整个进程 RSS 上限。1000×256 KiB≈250 MiB 只是等待缓存的算术示例，不是本轮准入配置。

整体恢复预算以[修订§3](../current/n1-session-rebuild-contract-20261013.md#3-唯一恢复事务与预算)为准：15s局部恢复＋15s清理观察＋90s startup，事务绝对上限120s/最多一次会话重建。阶段余量不增加attempts；旧资源未结算不得启动后继；所有资源跨代共用N0上限。原20s open、4s操作、15s tracer/shutdown期限不变。

### 6.2 策略约束

- N1 就要有一次恢复事务的总时限、并发合并和基本重试上限；N2 再深化黑洞与误判防护。
- 单次业务操作的重试预算与长期后台再探测分开：前者必须终止，后者可以低频持续，但任务、频率、排队和取消均有界。
- 每个重试层计入总预算，防止业务 open × pool replacement × Transport reconnect 乘法放大。
- 新增退避/抖动需明确修订旧策略；测试使用受控时钟和可重复抖动输入，不能照抄任意默认秒数。
- QUIC 活着不等于 TCP/UDP 业务可用。使用主动与被动证据，但任意 Target 失败不自动驱逐整个 Upstream。
- 不向透明业务流注入探测字节；受控探测只用明确的测试/配置 Target。探测成功不能掩盖持续业务失败。
- 不用空闲 payload 超时关闭健康 Established；有具体未完成工作或明确失败时按对应生命周期处理。

## 7. 恢复与质量验收口径

使用同一客户端单调时钟记录：`t_fault`（受控故障注入）、`t_detect`（判定）、`t_ready`（替代路径满足准入）、`t_tcp`/`t_udp`（首次真实往返）。无法观察真实故障起点时只报告可测区间，不混入受控故障样本。

- 发现：t_detect−t_fault；准备：t_ready−t_detect；业务恢复：t_tcp/udp−t_fault。
- TCP 和 UDP 分开报告。应用恢复播放另测，不并入 Transport readiness。
- 历史自动备用草案（随 N3 延期）：有可用备用、受控 RTT≤150 ms 时，明确故障业务恢复 P95≤3 s，静默黑洞 P95≤5 s。**这不是已接受 SLO 或成绩，不能用作本轮深圳↔US承诺；未来重启 N3 时须重新验证。** 本轮所选节点恢复阈值按实际链路与已批准预算单独冻结。
- 本轮记录所选节点重新上线的恢复；冷/热备用自动切换样本归延期 N3。N1 中服务器仍不可用的时间单独记录，不能承诺在不可恢复期间成功。
- 冻结 peer/配置、方向、RTT/丢包/MTU、负载、payload 尺寸、采样频率、重复次数、每轮 deadline、最大失败率和所选节点误判/非必要重连率；自动跨节点误切率归延期 N3，不能只填恢复秒数。
- 所有尝试都进入总样本，报告成功数/总数、超时、P50/P95/P99 和最大值。超时作为超过 deadline 的样本/删失数据披露，不能删除后只报成功样本分位数；样本不足不能宣称尾部概率已证明。
- 同时报告稳态吞吐、UDP 丢包/抖动、CPU/RSS、queue/task/connection 峰值、事件循环延迟和清理结果；不能靠频繁重连掩盖稳态质量问题。

N0 先冻结 N1 所需工作负载与阈值；N2—N5 在各自启动前冻结新增验收条件。未知值阻止相应验收，不要求 N0 提前设计所有后续实验。性能目标如涉及 >30/>100 Mbit/s，必须执行现行容量和端到端热路径可达性门禁。

## 8. N1 首条完整路径与测试矩阵

### 8.1 首个集成测试的行为

1. 启动受控真实 TUIC peer 与 TCP/UDP echo Target，实例化将被公开调用的同一客户端生命周期 Implementation。
2. 建立 TCP 并完成双向校验；从固定本地 UDP 四元组完成往返。使用带序号/内容校验的测试数据。
3. 使当前真实 Transport 连接失效，保持 peer 可重连；保留能够注入的旧 generation 迟到事件。该首测只证明明确故障，不冒充静默黑洞检测。
4. 同一持续服务 owner 驱动恢复：局部 connection 恢复不替换会话；明确会话级故障可在旧资源证明结算后最多构造一个后继会话。旧 TCP 可结束，新 TCP 与同一本地 UDP 四元组后续新包完成真实往返。不得通过新会话清零旧债或绕过当前接入丢包。
5. 旧包/分片/完成回调不能进入新关联，也不能关闭新资源。不得重放旧 TCP 字节或过期 UDP。
6. 请求正常 shutdown，等待实际任务与资源结算，检查消息/独立求和与存储字节、关联、连接、任务和关闭结果；不只检查 Connected 标志或一次 echo。

这是行为规格，不是一个已经存在的测试名。N0 定位真实测试 Seam 后填写全限定测试名和命令；禁止测试过滤器匹配 0 项仍算通过。

### 8.2 与首条路径一起闭合的处置

| 分支 | 故障/事件 | 必须观察 |
| --- | --- | --- |
| success | 局部连接恢复或有证据的会话级重建 | 服务owner不变；局部不换session，整体在旧资源结算后只换一次；新TCP/同四元组UDP真实往返 |
| rejection | 配置/认证/准入被拒 | 不发布可用连接，原资源/失败结果可追踪 |
| pressure | 连接、等待字节或任务预算满 | 无额外隐式分配/任务，拒绝或背压正确 |
| concurrent | 多个流同时要求恢复 | 一个相应恢复事务，总工作不乘法增长 |
| cancellation | 等待恢复或 join 的观察者取消 | owner 仍持有实际工作，重入不重复恢复/关闭 |
| failure | worker/join/Transport 返回真实错误 | 保留原错误并稳定观察，不默认为 clean |
| shutdown | 恢复中、已替代、失败中退出 | 停准入、完成已拥有的清理、无悬挂工作 |
| isolation | 旧代际、ID复用、单flow/slot失败 | 局部不影响健康工作；仅会话级证据允许该会话整体中断，记录原因/身份/计数 |
| incomplete | 清理15s未完成、未知资源或sticky quarantine | 保留并继续驱动旧owner、非零失败记录、后继构造0；120s事务不重置，不冒充进程重启 |
| hot path | 经选路进入 TUIC/REALITY | relay 类型与配置吻合；TUIC 不意外丢 D16 |

内存 fake 可用于策略/时钟单测，不能代替真实协议路径。真实 peer 测试不能复制另一套 test-only 成功实现。必须另有公开入口→同一 Implementation 的调用覆盖/源码证明；本地 loopback 不宣称 macOS TUN、WAN 或成熟 peer 全矩阵已通过。

### 8.3 每片门禁顺序

1. 记录源码起点、既有失败和 gate 顺序，运行与本片相关的 baseline。
2. 需要 Move 时先保持行为：characterization GREEN→Move→GREEN，不能与新行为混写。
3. 一个行为 tracer：预期 RED→最小完整 GREEN。既有行为 characterization 直接 GREEN 时如实记录，禁止故意造红。
4. exact 及有意义的串行重复、对应生命周期/公共路径回归；重复次数在执行卡固定，重复成功不是竞态不存在的证明。
5. scope、格式、whitespace、库/二进制编译、growth/provenance；随后独立审查并按已授权流程复测修复。所有接受证据必须对应最终源码版本。

以下是现有门禁命令模板，**本次文档任务未运行**；只在执行卡授权且环境就绪时逐条运行，首个意外失败后停止后续命令：

```sh
cargo fmt --all -- --check
git diff --check
cargo check --lib
cargo check --bin mini_vpn
cargo check --all-features --lib
cargo test --all-features --test code_growth_governance -- --nocapture --test-threads=1
cargo test --all-features --test repository_test_module_provenance -- --nocapture --test-threads=1
cargo test --all-features --test knife16_owned_leg_provenance -- --nocapture --test-threads=1
```

Integrator 从当前源码列出 affected-module 的确切测试名/数量与 default/all-features 覆盖，补足 TUIC、relay/D16、UDP、fake-IP/DNS 等回归。上述命令不替代行为测试，也不意味着每个小片都重复全部无关重型矩阵。新增 untracked 文件需单独 whitespace 检查；git diff --check 不覆盖它们。冻结流程若还要求 standalone governance，复用既有命令。

发现既有/无关失败也必须记录并由 Integrator 明确其归属；不能自行排除必需门禁或称全绿。全仓格式检查失败不授予全仓格式化权限。若保存的增强 WIP 阻断共同构建，提出保留现场的隔离或最小修复方案并按授权执行，不能私自删代码或修改测试预期。

N4 必须完成第5节修订后的所选节点完整本地矩阵；N5 前 STATUS 必须明确新兼容模式 readiness，并取得外部执行授权，且已完成 source/binary/config、服务/资源、观测、故障注入与恢复清理的 one-shot checklist。旧外部脚本只能审计后复用，不能直接运行旧 M2/Knife9 验收并改名。

## 9. 持续执行、停止与审查

### 9.1 可以连续推进的条件

同一已授权切片内，按冻结序列推进；跨切片必须同时满足前片接受、依赖满足、下一片范围与门禁已授权。不能把“继续”推导成对整条路线的无限写权限，也不应对尚有效的明确授权反复索取。

执行卡应在开工前一次列明所需 tests、provenance、格式和 Integrator ratchet 操作的归属，减少机械修改意外越界。现行 **repair-test 失败先分析并等待用户确认** 保持不变；本文不预授权失败后的第二次修复。若要扩大常规修复自治，必须另外明确修订规则，不能借“持续执行”暗改。

### 9.2 状态及处置

| 状态 | 含义 | 下一步 |
| --- | --- | --- |
| Planned / AwaitingAuthority | 设计或范围未批准 | 只读澄清，不开始写入 |
| Implementing | 范围内实现与预定测试 | 仅按执行卡推进 |
| ExpectedRed | 预测的缺口被准确观察 | 只有已授权 GREEN 才继续；不当成已接受功能 |
| FocusedGreen | 小范围测试通过 | 继续已授权后续门禁，不宣称阶段完成 |
| ReviewPending | coherent 产物已提交审查 | Writer 停止写入，Reviewer 读实际代码与证据 |
| BlockedUnexpectedRed / RequestChanges | 非预期失败或新的审查阻塞 | 停写，报告失败树、精确范围和最小修改计划，等待确认 |
| RatchetPending | 唯一失败为 oversized shrink inventory | Writer 报精确行数；仅获授权 Integrator 改 inventory 并复跑 |
| AcceptedLocal / AcceptedExternal | 对应层级全部义务已通过 | 更新清单、结果和下一授权动作；不跨层级推导 |

ExpectedRed 必须匹配预期符号/断言与测试数量；fixture/import/privacy/无测试命中不是有效业务 RED。修复后出现新失败必须重新分析，不能连猜补丁。

反复出现同类所有权/取消/关闭漏洞、同一不变量跨三个独立 owner、或无法在一处结算时，执行 AGENTS 的结构收敛门禁。收敛必须指出实际阻塞和可执行出口，不能以更多设计文字代替实现。

### 9.3 独立审查的输入与结论

Reviewer 阅读实际 diff、相关完整事务、真实调用者和测试，而非只读 Writer 总结。重点检查目标一致性、资源所有权、错误保留、总容量、UDP 身份、Native/D16、未授权变化、缺失测试和旧失败。

输出 findings-first：问题 ID、优先级、精确位置、可触发场景、验收缺口；无问题也说明范围和未运行的测试。Integrator 对照最终源码验证 closure。架构评分不代替门禁，不要求追求抽象 10/10 才能开始一个合格小片。

若指定同一 Reviewer，默认延续。原 Reviewer 不可用时先请求替换授权，转交旧问题 ID、证据和未闭合条件；替换不能清除历史 Request Changes。

## 10. 单切片执行卡与证据模板

一片一张卡，放在当前阶段结果/执行记录中，不另建多份计划。目标、范围、授权和门禁等计划字段必须在开工前填好；实际结果、审查与最终行数等证据字段先标“待执行”，只在真实发生后填写，不预测为通过。不得凭本文推测获准的 Rust 路径。

```text
slice_id / N阶段 / 状态 / 更新时间：
授权依据及仍有效的限制：
目标（一个可观察行为）/ 非目标：
依赖与待关闭的 blocker：
起点：branch、HEAD、tracked/untracked快照标识、已有失败：
Writer / Integrator / Reviewer 身份与实际工作位置：
allowed-files（精确路径）、symbols、允许的行为/格式/测试变化：
禁止修改项；Integrator-only inventory；commit/push/external权限：
真实入口→调用者→owner→Target→回程；将保留/删除的旧路径：
success/rejection/pressure/cancel/failure/shutdown所有权处置：
容量、时限、超限行为、UDP身份/复用规则：
baseline命令、exact测试全名/预期数量、RED预测与反证分支：
GREEN范围；重复/回归/格式/编译/治理顺序与stop rule：
实际结果：命令、退出码、测试数量、源码标识、日志位置：
Reviewer问题ID与closure；未执行门禁及原因：
最终路径/行数、oversized shrink、保留的WIP：
下一条可执行动作及是否已获授权：
```

每次证据更新只引用一份源码版本；代码变化后旧结果必须重跑或明确说明不受影响的依据。不得把不同版本的绿色结果拼成不存在的“全绿”。最终阶段验收使用完整一致版本。

## 11. 可轮换的小团队与持久化状态

### 11.1 角色与单 Writer 规则

建议主任务采用 1 Integrator＋1 Writer＋1 独立 Reviewer＋确定性检查；这是待启用的组织方式，不是本旁支运行的 Agent。

- Integrator 维护目标、范围、清单、证据、授权和集成；不自行降低验收要求。
- Writer 完成一片，不修改治理 inventory，不越界追加行为。
- Reviewer 原则上只读；审查期 Writer 停写。scope 内发现的问题也按修复授权处理。
- 检查机制做范围、文件规模、测试结果、交接完整性核验，不自行批准架构或继续红灯任务。
- 第二 Writer 仅用于经确认不重叠的 Module；worktree 隔离不等于架构独立。默认不启用。

### 11.2 安全交接

1. 旧 Writer 在明确状态停止写入与在途修改命令，保存第 10 节记录；允许红灯交接，不为交接擅自修复。
2. Integrator 核实旧执行者已停止，记录当前唯一 Writer 与 scope；一条状态文字不是自动锁。如宿主无法核实停止，不启动新 Writer。
3. 新执行者只加载最小交接包及相关源码，核对现场与证据；先复述状态、权限、阻塞和下一步，不从聊天里的“确认”推导新权限。
4. Integrator 自身轮换时也同样交接；旧主控不能同时继续派发。
5. 不靠会话名字保持正确性。上下文比例只有可可靠读取时才使用；否则以稳定边界、压缩后失真或约束遗忘为信号。

N0 做一次有限演练：停止并移交一张真实 N1 准备卡，验证新 Writer 不越界；主控仅凭保存材料恢复下一步；用明确标为演练的 blocked 记录验证换会话不会洗掉红灯；Reviewer 检查一份刻意缺证据的演练卡能否拒绝。演练不启动未经授权的写者，不模拟成真实生产结果。不建设独立 Agent 调度平台。

### 11.3 记录与巡检

- STATUS 保留当前接受位置、stop、最小证据入口和下一 gate；HANDOFF/TODO 只导航；结果记录命令与审查；历史移入既有归档机制。归档另按授权，不在旁支顺手清理。
- 不在多个文档重复逐条流水账；当前上下文必须能快速回答“在哪、为什么停、下一步是什么”。学习记录只保留会改变后续行为的规则。
- 每次交付、失败、范围变化和交接都检查。每完成三个切片或一个阶段，问：消除了哪个真实恢复 blocker？若只增加文档且无法指出 blocker 减少，触发一次整体复盘。
- 定时巡检可在另行启用后每 15—30 分钟检查停滞、重复 Writer、缺授权或证据；优先确定性检查，状态无变化保持安静，只有可行动变化通知。
- 先验证宿主实际支持的任务状态、定时、交接与恢复能力，不承诺未验证的事件回调、上下文百分比或原子写锁。本文件不创建 automation。

## 12. 投入预算、完成判据与启动文本

### 12.1 时间只作决策预算

以下为 N3 延期前的历史低置信度预算，不作为本轮工期承诺，须按实际进度重新估计：N0 1—2、N1 4—7、N2 3—5、N3 4—7、N4+N5 5—9 个有效研发日。首条路径按 5—10 日设复盘点，完整目标暂按 4—7 周预算；8—12 周也不是承诺上界。

“有效研发日”不是 AI 运转 24 小时的换算。执行后分别记录实现、测试/重跑、review、返工、等待授权的实际耗时与日历跨度；N0、N1 结束各校准一次。若 N1 明显超出预算，提交 blocker 与取舍，不自动延长或减少质量义务。增加 Agent 不将工期等比例缩短。

### 12.2 最终完成判据

本轮 N1、N2、N4、N5 义务在明确 source/binary/config 下接受；所选节点启动失败时有界结束并给出明确错误，由用户自行选择其他节点；所选节点运行中失效且可重新连接时恢复新 TCP 与后续 UDP 通信。持续 UDP 与迟到隔离通过；健康流不误杀；总预算与长期质量合格；无未经处置的关闭错误或未知资源；独立审查无阻塞问题。自动跨节点恢复及回切归延期 N3，不阻塞本轮完成。最终结论为“兼容模式恢复”，不保证原应用连接无缝续传，不冒充增强 R7/M2。

若只完成 N1，报告“单 Upstream 本地恢复已接受”，不报告全目标完成。文档批准、评分、hash 和测试总数本身都不是联网能力。

### 12.3 可发送给主任务的启动文本

以下是用户可选择发送的请求模板，文本本身尚不是已执行授权：

> 以本指导文件启动 N0。先核实当前 Writer 停止并保留完整 P3/WIP 现场，不要求先完成整个 R7，不删除或回退现有成果。只完成目标/ADR 适用范围、真实兼容路径与 owner、N1 必要预算和验收口径、可恢复基线及最小交接验证，形成一张精确 N1 执行卡并独立审查。不要开始 N1 Rust 实现，不运行 TUN/VPS/WAN/M2，不 commit/push，不新建定时任务。关键范围无法成立时提交取舍，不无限追加前置文档；现有停止规则继续有效。

### 12.4 本指导文件的自检与参考

方案自评分（非独立验收）：37signals 执行组织 8/10；Clean Architecture 8/10；System Design 7/10；DDIA 故障处理 8/10；渐进迁移 7/10。进一步提高依赖已批准的 owner/范围、填完的预算、真实 N1 测试和交接演练，不靠补更多文字或 Agent。

本轮沉淀：用同一恢复路径区分 TCP 新建与 UDP 关联替换；把未测时延视为假设；持久化失败和未授权状态；将未来组织建议与本次文件写入权限分开。按本次最小范围，这些要点留在本文，不改共享 .learnings 或 SKILL 文件。

外部资料用于原则校验，不替代项目实测或冻结契约：

- [Shape Up：投入预算与范围](https://basecamp.com/shapeup/1.2-chapter-03)：有限投入不意味着降低安全/质量要求。
- [长任务增量与交接](https://www.anthropic.com/engineering/effective-harnesses-for-long-running-agents)、[Harness 的简化与独立评估](https://www.anthropic.com/engineering/harness-design-long-running-apps)：按实际需要组织，不强制频繁重置所有会话。
- [Envoy 故障检测](https://www.envoyproxy.io/docs/envoy/latest/intro/arch_overview/upstream/outlier)、[sing-box URLTest](https://sing-box.sagernet.org/configuration/outbound/urltest/)：区分探测、业务证据和已有连接处置；不照抄默认阈值。

创建时只核对文档、既有规则与关键源码；未运行 Rust/Cargo 测试、未做独立 Agent 审查、未激活新主线。后续接受与执行证据由正式 N0/阶段结果负责。

## 13. 2026-09-18 N0 授权与范围修订

用户明确授权本会话执行 N0-A/B/C 的准备、取舍和开工验收，未授权 N1 Rust、commit/push、外部网络验收、TUN/VPS/WAN。当前 root 是唯一文档 Writer/Integrator；协作者仅只读核验。原仓库与 recovery 备份保持只读。不恢复旧 P3 的持续实现授权；其现存成果与全部剩余义务保留。

用户已明确回复“确认 D1–D4，继续 N0”，以下 D1–D4 **已批准为本轮范围修订**。批准只确定 N0 的范围与 ADR 适用关系，不自动授予 N1，也不把未填预算或不可用 fixture 变为通过。下表与 ADR 原文冲突时仅在本轮兼容恢复适用该窄修订；原增强/M2结论保留。

| ID | 用户已批准的具体取舍 | 影响与保留 |
| --- | --- | --- |
| D1 | 近期验收名为“标准 TUIC 兼容模式恢复”：保留健康 TCP；失效 TCP 有界结束；恢复新 TCP 和同四元组 UDP 后续新报文 | 不重放结果未知业务；不承诺原 TCP Target socket/NAT 映射连续；P3/Case C/R7/Task4、ADR-0015 增强义务与原 M2 失败保留，增强新增实现暂停 |
| D2 | N1 单 Upstream；N3 两个成熟 TUIC A/B；REALITY 继续仅 TCP；UDP/QUIC 完全封锁后的 UDP 恢复不属本轮 | ADR-0011 §1–2 的“UDP 永绑 TUIC”保留于旧 TUIC/REALITY 配置；新 A/B 允许换到另一个具备 UDP 能力的 TUIC，不发往 TCP-only REALITY；App/四平台发布不入本轮 |
| D3 | ADR-0011 的 TCP 选路冷却、一次恢复事务、长期后台再探测分层；后两层允许按 N0 冻结总 deadline、合并请求和有限重试，不沿用“无指数退避”约束所有层 | 源码已存在 500ms 指数退避至 30s；D3批准当时未包含新增预算；具体预算随后由同一N0卡独立审查冻结，仍是未来验收上限而非当前实现保证。慢 open 离开 TUN 主循环，校正 §4 纯 TUIC inline 旧文字。ADR-0014 健康空闲保护、90s pending write、10s half-close 保持 |
| D4 | 新兼容恢复与增强 M2 分别验收；指南 §7 的 3s/5s 继续为假设，不作为已成立 SLO | N1 只先冻结明确连接失效且 peer 可重连的本地样本；黑洞/双出口/吞吐各按 N2–N5 门禁。未知性能如实 Unknown，不能取消未来恢复与质量阈值 |

N0-B有限设计出口：唯一N0结果的同一N1卡已整合公开session/每节点TuicUpstream owner、slot0/aux、恢复事务、producer-bound UDP身份、stop/join、六类预算与精确scope；真实peer版本/配置准备已通过，同一卡已独立最终Approve0/0/0；N0完成，仅冻结N1设计，不授予Rust权限。不得再新增同义架构 sidecar，亦不得先实现再补批准。

真实测试环境最小准备候选 F1：仅当前副本内建立隔离 localhost 测试身份与适配 Darwin arm64 的固定版本/hash TUIC peer；不覆盖 `/usr/local/bin/sing-box`，不使用旧私钥。现有测试硬编码 `certs/dev`，若复用现有生成脚本，需保存两个公开证书原字节、临时安装新测试证书/私钥、运行 exact 基线后无条件恢复公开证书并删除临时私钥；私钥不进检查点/日志/Git。peer 仅绑定 loopback，保留进程退出与回收记录；下载来源/版本准备另明确后执行，不启动外部网络验收。这是最初F1方案；实际第一次执行失败及F1-R1停止位置见下段。

F1当前状态：Darwin arm64 sing-box 1.14.1已下载并验证官方digest/version；第一次临时证书准备触发CaUsedAsEndEntity，已恢复原证书/删除私钥，按repair-test stop rule暂停后，用户已回复“确认”授权具体F1-R1；F1-R1随后通过链/SAN/key匹配、两模式8条冻结基线及peer loopback启动/退出，原证书恢复/临时私钥删除。详见唯一N0结果末尾和evidence/authority.json。

### D5 已批准 B：异常原对象与严格字节上限

whole-scope审查发现ClientError/io::Error/JoinError可携带任意opaque或panic对象；“完整保留任意原对象”和“固定完整字节上限”不能在不限制错误域/表示的前提下同时保证。用户随后明确回复“D5-B：严格字节上限，接受明确的有损错误表示”，选择下列B；A未采纳。不得用诊断ring的上限冒称原错误内存上限。

- **D5-A：保留完整原错并显式限定预算承诺。** 正常已建模错误域逐类证明字节上限；未知opaque/panic错误保留原对象，进入sticky quarantine、停新准入，按未结算资源slot限制数量。明确未知opaque payload字节为Unknown，不能声称完整错误域或整进程内存已证明有界。这是对当前预算验收的窄例外，须用户批准；不降低原错误/非零失败保留。
- **D5-B：批准严格有界错误表示。** 完整保留来源/阶段/typed cause/code及资源身份等结构化事实，但对动态文本/opaque payload定义明确上限、截断/不可表示标记及处置，不再承诺保留任意原对象。用户已明确批准这一错误保留契约变化；同一N1卡给出具体字节/slot值与测试，不能只称日志截断。

最小公开生命周期边界也须纳入N1卡：main先拥有非克隆会话owner，再借用它驱动startup/run；Ctrl-C/device fatal/lifecycle fatal走同一个stop/cleanup。持久loop state、TUN pump、open/relay registry、TUIC owners全部在该owner之下，观察future取消不销毁owner。device pump的阻塞read与reserve都可stop，真实join结果归device；顶relay必须保留子join失败。候选scope因此包含device.rs/main.rs及原client_tun入口；结果同一N1卡已列精确候选范围，尚未授予Rust写权限。它是标准公开路径的必要前置，不要求实现P3，也不包含实际TUN外部验收。

D5-B仅约束控制边界保留的错误记录；不得任意格式化未知Display或保留原opaque对象后声称有界，也不推导外部runtime瞬时分配/RSS有界。具体record字节/slot数量、首错与完成ring在同一N1卡冻结并复核；N1仍未授权。

## 14. 2026-09-18 N1 启动

用户在确认N0完成、询问多Agent分工后回复“继续”；本会话按冻结N1卡执行M0/M1/M2及N1-L/R，权限和当前Writer见n1-20260918-evidence/authority.json，[实际N1记录](2026-09-18-compatibility-recovery-n1-results.md)承接命令与审查。原N0接受和卡的语义不改，原“未授权”是该时点记录。未授权N2–N5、P3行为、commit/push或外部/TUN/VPS/WAN/M2；repair-test stop保持。

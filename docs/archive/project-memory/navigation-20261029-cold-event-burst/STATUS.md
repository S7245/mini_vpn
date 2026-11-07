# N1 当前状态：Native 固定 18 格逐项验收；两 guard 关闭

**前置只读可行性门：NO-GO直接接候选。** 本轮仅为N1-NATIVE固定18格的剩余联合验收；N1-FAMILY-WF/N1-S1-JOIN只可修直接阻塞接缝。独立Reviewer核对原full matrix：`retire15`返回后才启动新5秒accept/AUTH观察，TCP/UDP各在后续原观察；`C+H≤5`只是必要非充分，B820/B830/B832的旧close前+真drain已分别约5.359/5.119/5.021秒，串行必超时。获批一枚未发布无DATA候选重叠只可能缩短AUTH关键路径，DATA仍须旧双witness；任意RTT/PTO无固定恢复保证。预算分区虽已批准，审查当时Quinn endpoint/datagram/defrag/ordered/source仍先分配后计费，Quinn双向事件通道无界且无保留drain回执的满队列协议；Node与Device最终发布也无同Service Root的不可失败线性化提交。**停止生产候选接线**，先证明分配前额度、owned队列有界可唤醒协议及共同提交可编译且六处置闭合；正常队列压力不得拆掉同Node其他Flow。审查详见[唯一计划](n1-f1-implementation.md)及[统一结果](n1-resume-20261017-evidence/b560-result.md)。该只读轮没有生产变化或新验收GREEN，固定18逐项18A/0O、父23=9A/13O/1B及两guard均不提升。B842原真Service探针仍RED；B835串行30/30不能覆盖高RTT或B556默认并行。原5/15/PTO、首错、charged Q、单Writer/独审、代码增长门及全部禁令保持。

**2026-10-28 有限生产进展：** B851–B866在Bytes fork加同一布局硬额度原子预扣/sticky Q、预算化inline ordered copy与可失败`BytesMut` datagram复制；Quinn原非空`poll_socket`改为**先预扣再做原两次分配**，空包仍无动作。真本地Endpoint预算meter UDP接收B861 RED→B862 GREEN，B863原Quinn三项3/3、B864 Bytes13/13、B865空值API1/1、B866治理7/7；独立Reviewer非满额路径无P0，空值P1已修。**仍NO-GO把预算meter接到生产Node或创建第二Endpoint：** 满额现在会经IO Err终止整个Endpoint driver，违反同Node Flow隔离；须在原RecvState预分配保留批次/meta/stride和socket/rebind身份，额度释放无丢唤醒，保`handle_events`推进drain。Quinn双向事件队列、defrag/source、Service最终提交及真实六处置仍开。此轮固定18、父23与两guard零提升，未证资源继续Q。

**2026-10-28 本轮可行性复核：仍NO-GO生产候选。** B867–B873将预算复制拒绝分为可恢复`BudgetFull`、永久`TooLarge`、`Quarantined`与`Allocation`；B874 Bytes16/16、B875真本地Quinn3/3、B876治理7/7，均仅证明错误分类及非满额接缝。独立Reviewer确认原后继5秒从`retire15`返回后起，B820/B830/B832的旧close前+真drain已越5秒；获批候选只能缩AUTH，旧双witness仍先于DATA。`RecvState`满额仍终止整个driver，双向事件通道无界，recv_buf/defrag/source/owned queue尚无共同前扣及释放回执；`NodeHost::data`、UDP source、Device resume/TUN pump也未受同Service Root线性化提交控制。按结构门停止第二Endpoint和Service候选生产接线，先证明原RecvState保留/唤醒、事件队列有界drain、同Root预备/最终发布可编译且六处置闭合；无法做到则使用唯一计划中的具体合同取舍。固定18仅逐项18A/0O，联合未过；父23=9A/13O/1B、两guard关闭。

**2026-10-28 Full结构反证及获批A1后的有限处理。** B877真Endpoint Full后driver不恢复RED，B879固定Pending独立退额有限GREEN；B883成功commit的release(0)假唤醒RED→B884修复。独审确认全Endpoint Pending可挡ACK/STOP/CRYPTO，`R-HARD-BUDGET-ADMISSION`第三次触发结构审查。用户已批准A1：仅在硬额Full时有界计数拒绝未入Quinn Proto/Root、未入账的UDP datagram；已入账债、首错、charged Q、原5/15/PTO、双witness保持。B891统计API RED→B892真UDP GREEN，旧Pending/waker移除后B893/B894复绿；B898真QUIC同Endpoint两stream第一backing持有、Full至少一包计数、退额后第二精确交付、driver预算零，但不能把被丢包归为第二DATA或证明Root两Flow。B902小预算构造RED→B903在Proto Endpoint创建前按recv_buf+最大合法GRO复制布局冷拒绝；B904旧测试1024配置不足RED→B905修正测试预算后6/6。B906更大段数rebind真RED→B907在同State锁换socket前拒绝，旧socket仍收/代0/预算终态零，独审无新P0/P1。B908 Quinn7/7、B909 Bytes16/16、B910治理7/7。**仍NO-GO接生产Node或第二候选：** 双向Quinn事件节点无界、`Drained+endpoint_released`缺终态专槽，defrag/source及其它受控库存未全先扣，同Service Root最终发布未闭合；固定18联合、父23=9A/13O/1B、两guard均不提升。

**同轮队列结构复核（只读；无新运行验收）：** 独立Reviewer纠正了初次单行检索漏掉跨行`push_back`的结论：反向Proto在真实client路径可产生`NeedIdentifiers`、`ResetToken`、`RetireConnectionId`，不能把反向通道收窄为仅终态槽。两向普通事件与Proto内部`VecDeque`须在状态变更前有界准入，每连接另保不可占用的`Drained+endpoint_released`槽；Endpoint删真实map后ACK。`response_buffer`/send scratch及defrag临时新旧库存也须前扣。审查尚未证明原硬预算合同不可实施，但现API无可编译完整边界，继续禁止候选生产接线；先收敛此单一Quinn连接登记owner与Proto容量接缝，再按唯一计划推进其余阻塞。只有文档中的精确结构结论更新，未改生产/测试、未运行新测试；固定18联合、父组和两guard不变。

**同轮 Proto 准入纵切（代码与测试已执行，尚非硬界）：** 原 `Endpoint::handle` 行为保持地移入私有深模块；新增已知连接只读路由后的 `handle_with_admission`，允许构造 `ConnectionEvent` 前拒绝且不改 Endpoint index/连接状态/response。仓库外同源 Proto 真加密 PING 拒绝与同ch重放1/1，原根仓 Quinn7/7、增长治理7/7，独立 Reviewer 无当前 P0/P1。第一次 `cargo test -p quinn-proto` 因 vendor 不属workspace仅命令RED，归既有同类 vendor 调用结构门，以仓库外临时manifest复绿。随后将布尔回调收敛为 `AdmittedDatagram<T>`，token 与真实连接事件同返；Proto定向1/1、Quinn7/7、治理7/7，独审无当前P0/P1。此接缝尚未被生产 Quinn 调用，泛型 token 也不自证预算，**不能宣称双向队列或物理预算闭合**；下一在同 Endpoint/Connection owner 中把线性 permit、普通有界队列与不可占用的 Drained 终态槽一起接入。生产 Node、第二候选、联合验收与两guard仍NO-GO；父23=9A/13O/1B不变。

**同一队列门的有限邮箱原语（尚未接原事件）：** Quinn私有每连接邮箱先扣`Slot<T>`物理数组；普通、Rebind、Close及Drained专槽有线性permit和预留时序号栅栏，普通/控制任一先预留都不能被后提交事件越过，终态仍等待先前permit。仓库外同源Quinn邮箱12/12、根仓check和增长7/7；独立Reviewer未见当前原语P0/P1。控制布局变更曾使旧测试断言9/10 RED，仅修测试后同组复绿；新的预留交错真实RED→GREEN保留raw。**原Endpoint/Connection仍用双向无界mpsc**，控制槽尚未在换socket/close前全连接预留，反向Proto事件、原Drained→map移除→ACK、完整硬账和Root receipt均未接线。固定18联合、父23=9A/13O/1B、两guard不提升。

**2026-10-28 正向 owned 邮箱接入的有限边界：** Quinn 预算化连接在 Proto connect/accept 前先取得64普通+Rebind/Close专槽（连接数上限19）；已知连接 UDP 在原 `handle_with_admission` 回调内先取得线性permit，Full仅对未入Proto包按A1计数拒绝；`rebind_abstract`在换socket前全连接预留，失败保旧socket/代；反向事件在修改Proto前预留正向槽并有一枚待处理事件，driver Drop会丢弃待决回执发送端。原非预算连接继续旧通道。根包check、真本地Quinn7/7、仓库外同源邮箱12/12及增长治理7/7，独立Reviewer只认可正向有限进展：反向仍无界、Close部分失败仅Q而未证明全部stop/JOIN，response scratch及其它物理库存未闭合。**不得接生产Node/候选或提升固定18联合、父组、两guard。** 下一门为同连接登记owner的反向有界普通队列+不可占用Drained回执、Proto内部事件持有/背压及Close失败分枝，随后才审其余资源和同Root最终提交。原期限、首错及charged Q不变。

**反向容量门的新证据：** 仓库外同源Proto真加密包测试1/1：一枚原writer包在原接收链产生96个RetireConnectionId；独审确认固定64不足，未证明最大产量。本轮仅测试代码变化。下一核实际配置单datagram字节界与非报文来源的完整产量，按同硬额冷预留并在首次Proto修改前取得credit；反向先reserve再pop、Drained独占槽。当前Node1472B与Endpoint允许的1200..65527B均须按配置算成本，不能用active CID数量代单包产量。生产反向接线与候选继续NO-GO，父组及guard不提升。

**反向结构选择已收敛，尚未实施：** 独审推荐预算模式仅在真实普通EndpointEvent出现时同步调用原Endpoint结算并直接返回同Connection，以消除双邮箱循环背压；Drained/Drop仍用独占终态槽，Endpoint删真实map后ACK。必须证明Connection→Endpoint单向锁序、无同步析构重入、Proto操作前burst credit及NewIdentifiers分配前额度。原反例测得218B加密包→96事件、每事件48B（`n1-event-capacity-layout.log`1/1）；它不证明最大容量。本轮无生产行为变化，继续NO-GO候选与guard。

**普通反向同步入口已有限接入：** 预算化Connection的普通EndpointEvent同步交原Endpoint owner，NewIdentifiers直接返回同Connection；无预算模式保持旧路径，Drained仍deferred。独审两P1已修：connect/accept先释放Endpoint锁才spawn，反向结算/认证通知失败传播原driver终止且meter.Q。真Quinn7/7（Full两stream用同步首次poll Runtime）、仓外邮箱14/14、增长7/7；独审未见上述修复新P0/P1。连接引用存活时terminate不能冒map释放，继续charged Q。终态冷槽、Proto内部突发前置容量、CID分配及控制公平性仍开；Node候选、父组及两guard不提升。

**预算化Drained冷槽已接入（有限签收）：** 原Proto连接创建前共同预扣forward与单终态Slot，预算Drained/Drop不经mpsc、不取Endpoint锁；Endpoint消费后仍原map删除→ACK。首版逐连接poll被独审认定热扫描P1，已收敛为固定19位就绪索引+单OwnedNotified，仅定位已就绪槽。真Quinn8/8（含额度满额拒连接不注册）、仓外邮箱/notify16/16、增长7/7；独审该P1关闭且无新P0/P1。通知前后注册交错/receiver丢失失败ACK已测；序贯单槽复用及无终态入槽时driver丢失已由真Endpoint定向2/2、完整Quinn10/10证明并独审有限签收；19槽同时入终态、两批全槽复用已由真实current-thread双回执证明；Drained先入槽再原driver IOErr真实RED（ACK失败但未Q）已在共同EndpointDriver::drop清活连接前置Q修复，原四项4/4、完整Quinn12/12/growth7/7，独审无新P0/P1。仅签这两个固定调度交错，不冒任意多线程或Native DATA。Proto内部事件库存、突发操作前额度、CID分配与独立实例核账仍开，候选/联合/两guard不提升。

**Proto CID冗余事件已收敛（尚无固定容量证明）：** 原CidState已删除过的退休序号现不再排空操作EndpointEvent，首次有效退休与原Endpoint补发保持。真加密活动CID重复96帧由96事件→1、原回复1，混合两CID保持首次顺序/回复2，admission3/3、原CID回归14/14、真Quinn12/12、growth7/7，独审无P0/P1并仅签去重。原seq0反例实际是已退休序号的96个空操作，raw仍保留；它不代表96个首次有效退休。timer/preferred/延迟NewIdentifiers可使瞬时集合超过peer目标8，禁止据此采用8槽。下一核首次有效事件burst与冷额度，Proto固定库存/CID分配/Root提交仍开，两guard关闭。

**Proto操作边界有限接入：** budgeted `process_conn_events`进入及每个Proto/Rebind/Close后同步排空普通EndpointEvent，Err仍原fail-stop，legacy原调度不变；真Quinn12/12、原CID及owner回归15/15、growth7/7，独审未见新P0/P1。原CidState128轮测试观察max=limit+1仅反驳8槽，非全局9槽证明。`retire_timestamp`按不同到期时刻保存历史批次，库存可与瞬时active数脱钩；不得直接落实小固定事件容量。下一在同CID owner共同核首次事件、待回送CID和寿命记录的固定库存/操作前额度，drive_timer/app/abandon及单个合并datagram仍须覆盖；完整硬界及两guard不提升。

**CID寿命库存反例已实证，停止剪枝接线：** 原owner在2秒lifetime内256次不同纳秒退休/补发，active8而历史记录257（`n1-cid-lifetime-stock-red.log`）；probe完整源码与仓外受测树保留，未以改断言冒GREEN，仓库生产/已接受测试保持本轮前边界。独审确认直接删除空批次会改变中间retire标记及后续prior_to，尚非严格等价。选择现授权内的原语义硬准入路线：Proto事务首次状态修改前取得事件/CID/寿命记录credit；不足只能按原A1在接管前拒绝或冷拒配置，不能处理半包后Q。下一必须形成该共同可编译边界才继续生产接线；若做不到则提交明确合同取舍。父23及两guard不提升。

**Whole-datagram准入落点已核定，硬容量尚未实现：** 原解密会原地改payload并提交key/timer，首包后再计数不能回滚coalesced前缀。独审核定现同步API须在`handle_event(Datagram)`首个stats/path修改前按实际first_decode+remaining长度取保守整包credit，并覆盖first_packet及timer/control来源；不能以1472广告或明文后回调代准入。已仅行为保持拆出原key提交，key_update2/2、真Quinn12/12/growth7/7且独审无P0/P1；未接拒绝/重试。下一核可冷取得的实际输入容量及共享事件/CID/寿命credit完整纵切，不能以小提取放行候选或guard。

**整包拒绝/原对象重试入口已真实验证：** Proto新增handle_event_with_admission，在任何旧处理前借原owned event取token，拒绝原样返还，成功token跨完整coalesced处理；datagram_len含first_decode+remaining。原加密PING/密钥更新、真实双握手合并包拒绝时stats/auth/timeout/TLS无变化，同event重试成功；准入5/5、Quinn12/12、growth7/7，独审无P0/P1。Quinn预算路径尚未调用此接口，token未绑定真实额度/槽消费，不称硬界。下一把实际输入/非报文burst的冷库存、CID/寿命credit及拒绝处分完整接入；候选/联合/两guard继续关闭。

**全范围复核已收敛下一完整A事务，仍NO-GO候选：** 唯一计划资源表已按代码纠正旧“正反事件仍无界/ordered copy仍后扣”的过时表述。A在现合同内原则可行，但必须把整操作事件/CID回复/寿命记录的credit与冷slots共同闭合；控制Full须保留容量/继续推进，不能拆同Node Flow。写范围和六处置退出已锁在原Proto/Quinn/Bytes owner；完成该边界后仍须defrag/source/typed队列、Service两代预留和最终提交。本轮只有只读全范围审查及计划/状态纠正，无生产修改或新运行GREEN；固定18联合与两guard不提升。

**冷槽owner已完成行为保持下移：** 原Quinn generic Slot/Permit/顺序/控制与终态机制及12个测试整体移至Proto隐藏owned_events模块，Quinn原适配复用同实现；冷规划与实际分配共用slot_layout。core12/12、wrapper4/4、真Quinn12/12、growth7/7，独审无P0/P1（跨crate可见接口P2保留）。首轮移动私有import编译RED已修，归原提取类第2轮。此为按门禁分开的纯移动，Proto生产者尚未接冷槽或消费credit；下一才改库存行为，候选/联合/两guard不提升。

**整操作槽credit机制已实现，Proto库存尚未接：** 共用owner新增BatchPermit一次reserve N槽，Full拒绝不改库存/序号，逐send消费并前移顺序fence，取消只返未用，terminal仍等更早信用。批量/原单Permit两个模式丢唤醒RED按同类第2轮累计，已收敛共同State提交：预留清且有空位才wake producer；core17/17、Quinn12/12、growth7/7且独审无P0/P1。原语GREEN不等于Proto生产者已消费credit；CID/寿命/事件冷库存与Root提交仍开，父组和两guard不提升。

**Proto冷事件生产者已实际接线（整A仍未闭合）：** 硬预算模式原endpoint_events VecDeque换为前扣Slot+Batch，Proto connect/accept在CID/TLS前取完整冷布局；Quinn传原recv_buf/BATCH的实际单slice上界，Standalone默认65535，冷容量为上界+16，整包在修改前reserve实际L+8。首次握手/timer/迁移入口预留，Drained专槽；已归Quinn的拒绝event先结算旧项重试一次，仍拒持原event并Q终止，不作A1丢包。accept buffered replay拒绝/Q均优先原首错、原Drained回滚未发布map后Err。Proto363/363、真Quinn12/12、growth7/7；独审实际L及accept错误成功两P1已关闭。L+8混合合法帧上界仍待完整证明，CID回复/寿命/registry及Source/defrag/Root提交仍开；未接预算Node候选，固定18联合与两guard不提升。

## 已接受边界

Step B 固定99叶=99A/0O/0B；N1 父23=9A/13O/1B，未完成14；顶层37=23A/13O/1B。父组计数不因固定清单中单格或单分枝通过而改变。原 5/15/PTO、首错、无法证明释放则 charged Q、单 Writer/独立 Reviewer、代码增长门和全部禁令保持。

固定 18 格的唯一执行清单及失败类别轮次在 [N1 唯一计划](n1-f1-implementation.md)，最新原始证据、GREEN/RED 和独审边界在 [N1 最新统一结果](n1-resume-20261017-evidence/b560-result.md)。当前已按固定标准签收：direct/成功（B709）、direct/拒绝（B705）、direct/压力（B243+B712+B717）、direct/fatal（B720/B721+B739）、direct/取消（B742/B743）、direct/shutdown（B747/B749）、ordered Chunk/成功（B756）、ordered Chunk/拒绝（B706）、ordered Chunk/压力（B762/B764）、ordered Chunk/fatal（B774）、ordered Chunk/取消（B781）、ordered Chunk/shutdown（B783/B784）、Permit/成功（B788/B790）、Permit/fatal（B793/B804）、Permit/取消（B800）、Permit/拒绝（B704）、Permit/压力（B650/B651）、Permit/shutdown（B692/B695）。18 格已逐项有限签收，联合验收仍开放；单格签收不得倒填为父组 Accepted。

**最新 B714–B718：** B714 真 wire Flood 丢 server 收到的 ACK，默认 peer cwnd 限制近4MiB写入，5秒 RED，归测试驱动 `R-DIRECT-WIRE-CWND-ORACLE` 第1轮、主父 `N1-NATIVE`。B715/B717 只在测试 server 复用原 cap 场景的 Cubic 8MiB 初始窗口；最终真 Service 同一 direct Flow 持原密文首包、近4MiB后缀进入 Node backing，App restore前0B，恢复后精确全量交付、四 JOIN、原绝对父期限内 Root F0/Node backing零，selector1/1。B718 治理7/7，独立 Reviewer 无P0/P1。结合 B243 原 owner 4MiB上界及 B712 越界至原父15秒 charged Q，**direct/压力限定这两条真路径 Accepted**；不声称 assembler 每字节峰值、Quinn 内部窗停进或所有拥塞配置。

## 下一可执行门

固定18格逐项限定Accepted；当前唯一下一门是[唯一计划的共同队列/提交边界](n1-f1-implementation.md)：Quinn双向普通事件在原Proto状态变更前取得同硬额容量或由唯一owner有界托管，且每连接`Drained+endpoint_released`须有不可被普通事件占用的终态槽；defrag新旧双份、UDP/source/owned queue等受控分配须先扣再分配，最后引用/原receipt退额；随后Node、UDP/health与Device cut receipt在同Service Root最终提交并发stop再验。上述完整边界可编译、可反驳且独审签可行后才允许一枚候选和六处置真测。若必要资源或最终控制权无法界定，停生产接线并作合同取舍；不得靠快照、调批次或碰运气重跑。之后依序高RTT串行、原默认并行、固定18联合、独审。`R-FULL-MATRIX-RUNNER`第3轮、`R-EARLY-CLEAN`和`R-ACK-PROOF-ORACLE`结构门持续；父23=9A/13O/1B、未完成14，两guard关闭。

## 保持关闭的门

两道 Native guard 在完整原 Root COMMIT/JOIN、Native 物理账、实际 join 的联合证明前关闭。R7 当前本地必要条件不构成吞吐或外部验收；公开 TUN 仍走 legacy upstream。不得运行 macOS TUN、VPS、WAN、吞吐或 M2；不得以测试注入冒生产可达性、局部退款或绕过 receipt。现阶段不提交/推送。

## 最小证据索引

- [N1 最新统一结果](n1-resume-20261017-evidence/b560-result.md)：最新 Native 固定清单取证、失败轮次及 Reviewer 签收；raw 根目录 `/Users/liushan/.codex/visualizations/2026/10/26/01a17639-85fe-71e1-8ea8-8af5b0671fb4/`。
- [N1 唯一计划](n1-f1-implementation.md)：18 格当前/缺口/下一测试、共同结算模型、结构门。
- [B74 原有限边界](n1-resume-20261017-evidence/b74-result.json)、[B138 Root/物理](n1-resume-20261017-evidence/b138-result.md)、[B243 direct source](n1-resume-20261017-evidence/b243-result.md)、[B529 JOIN 结构收敛](n1-resume-20261017-evidence/b529-result.md)：只在对应不变量需要时回看。

# N1 当前状态：读取准备拒绝保留原库存；完整冷账/Native Open，两guard关闭

## 当前任务与计数

本轮仅完成 `N1-NATIVE` 固定18格的剩余联合验收；允许修复直接阻塞的 `N1-FAMILY-WF`、`N1-S1-JOIN` 共用接缝，不自动接手这些父组的全部义务，不推进其它父组。

固定18格逐项有限18A/0O，**联合未过、N1-NATIVE仍Open**。Step B固定99叶=99A/0O/0B；N1父23=9A/13O/1B，未完成14；顶层37=23A/13O/1B。本轮未提升父组。B编号、文档增量、原语或单次GREEN不计完成进度。

唯一剩余工作清单、失败类别与跨模式轮次在 [唯一计划](n1-f1-implementation.md)；exact测试与原始证据在 [最新统一结果](n1-resume-20261017-evidence/b560-result.md)。不另建平行计划。

## 最新有效代码与证据边界

**最新必要修复（2026-10-30）：** 原Chunks::new先从map移出Recv再做ensure_ordering，普通IllegalOrderedRead和BackingAuthorityLost均会丢原接收库存。现所有可失败准备在原occupied entry内，成功才唯一移交Chunks；无新owner/退款/旁路。原真实加密两分枝RED保存，修后普通1024B先读128、两次拒绝保progress/used、余896精确交付/原双drain/maps0/used0；另一真实1B+人工Q拒绝保原map/backing，已知Drop后Q/未知债1。最终Proto403/403、Bytes17/Quinn12/provider2/growth7，独审无P0/P1，仅签read owner。不冒Native自然压力/18格；R-EARLY-CLEAN≥18→≥19，两分枝属同一次修复，不增父组。raw n1-read-prepare-*在唯一结果。

**完整冷库存的实现约束已复核：** reset/stop/read的实际预算入口必须由同Connection带显式now持准入/first/原Transport Close与3×PTO；Quinn持锁取runtime.now、错误返回前wake，Pair用原pair.time，禁止Instant::now/旧时刻/kill代正常Full。旧公开拆借view不得绕预算。Chunks须在实际消费/跨信用阈值之前取节点，Blocked/EOF/零读不能凭空触发Full；Drop只结算预持节点。PacketBuilder在populate前推进PN，故首个必要metadata须在Builder前准入，后续普通Full仅停止追加DATA并结算真前缀；mandatory control/CRYPTO split所有可能失败须前置，原ACK/PTO/重传不能因整包粗估Full受阻。这些是实现门，不是已接通的功能。

Session有界输出前一有限签收保留；生产Connection::write_crypto仍旧Vec。下一仍是原同Connection冷record pool/有界去重+producer/发包恢复/typed first/wake+Endpoint登记完整边界，随后source/defrag、同Service两代Root/最终提交及原联合顺序。不能自动把所有StreamsState状态改列硬布局；其独立实例/配置界仍必须核。未证明继续charged Q，两guard CLOSED。

**最新完整可编译准备（2026-10-30）：** 原Retransmits/ThinRetransmits及PacketSpace::requeue已行为保持同移至connection/control_inventory.rs（138行），原生产caller指向该owner；Thin私有字段、CID append→general merge顺序与原函数体相同，无wrapper/权限扩散。混合真RESET1/STREAM2移动前后均producer2alloc/发包3alloc，Proto400/400、Bytes17/Quinn12/provider2/growth7 exit0，独审仅签纯移动无P0/P1。原空白分隔符导致首个hash断言RED如实保存；共同核账改为精确函数内容，不改函数内字符，证据镜像子类1→2、非运行RED。没有metadata硬账/Quinn预算输出接线或父组提升。

**紧接着实现：** 同Connection单一冷record pool与有界去重索引，metadata credit在reset/stop/Chunks读取、PN/FIN/成帧和TLS key-only消费前取得；节点沿pending→sent→loss/Retry/0RTT→ACK移交，保原各队列顺序。MAX_STREAM_DATA陈旧ID清退与有效义务不能混淆；CID/general重入共同失败处置、typed first/driver wake必须一起闭合。provider新接口仍未被Quinn生产预算入口调用；完整库存/Endpoint/内部TLS配置界/Root仍Open。最新raw n1-control-inventory-*见统一结果；以下provider有限接受仍有效。

**2026-10-30当前有效边界：用户已选择TLS方案A，目标active，继续实施，无须重复询问。** 将同版rustls0.23.41纳入vendor/root patch；相对registry仅quic.rs行为差分。新增write_hs_bounded(slice)在任何pop/output/key变更前精确扫原barrier，短缓冲typed Insufficient、长度溢出typed LengthOverflow；成功与legacy共用原消费/key顺序，含空flight移交keys。无输出扩容/全局锁；keys和内部TLS库存的原分配并未消失。

**实际验证/独审：** 新provider2/2（含新旧接口四组合、每flight zero/n−1拒绝/足量成功/尾哨兵、真TLS双方Finished/参数及HS/1RTT各一次、空flight key handoff、拒绝后Drop）；根Bytes17/Quinn12/增长7；patched Proto400/400，均exit0，scoped fmt/diff通过。独审无P0/P1，仅有限签provider输出接口。初RED缺接口、GREEN编译漏子Module import E0425及精确修复raw保留，归现cold-interface编译3→4，不加父组。

**下一唯一门：** 同Connection cold control/输出事务接Quinn Session与write_crypto：先取得metadata/read/control额度，再允许空flight key handoff；非空短输出拒绝后，先共同硬额/Bytes真实布局分配，再写出并移同控制节点。现有Quinn仍用legacy，不能将provider测试当生产预算path。必须连reset/stop/Chunks/发包恢复metadata/Endpoint固定登记形成可编译完整边界；rustls内部send_msg先encode Vec→hs_queue仍按TLS实例/配置义务核，不冒本接口已约束整个CRYPTO峰值。后续source/defrag/typed/同Service双代Root提交及原六处置→高RTT→默认并行→固定18联合→独审全部保持；无外部run/commit/push。Native Open，未证charged Q，两guard CLOSED。

raw n1-tls-bounded-provider-final.log、n1-tls-bounded-proto-compat.log、独审/源SHA manifest见统一结果。前述TLS公开接口阻塞已有限解决，不再等待A/B/C选择；下文是此前诊断/接受范围，不撤已有raw。

**前一结构审查退出（2026-10-29）：生产接线NO-GO。** ACK/loss scratch与cold65的上一轮有限签收保留；其余metadata/Endpoint登记和CRYPTO输出尚不能形成当前接口下的完整硬预算边界。独审确认，原reset/stop在入队前已改stream/window，ClosedStream错误可能跳过Quinn wake；Chunks finalize/Drop消费后才排控制项。必须由同Connection控制权先取完整read/control credit、记录typed first并唤醒driver，pending→sent→loss/Retry/0RTT→ACK移同节点；Endpoint另持固定Meta/route/reset登记token，不能仅搬Retransmits或补容器cap。

**实际新增仅test characterization：** 原单datagram transmit的RESET+1/STREAM+2，peer真解密与两条128B精确交付，producer区间2alloc/发包区间3alloc，原双drain/map0。计数不能逐容器归因；测试wire未纳硬预算，final cold budget0不冒完整峰值。锁定rustls0.23.41真实provider在预留1B/Vec cap1后输出183B/cap183，第二次输出0（已消费队列）；此为明确API反例，非预算Connection，也不证明任何理论大上限都不可能。

**必要资源具体阻塞：** `crypto::Session::write_handshake(&mut Vec)->Option<Keys>`没有长度预检或有界可失败输出；rustls0.23.41私有write_hs先pop再extend，可隐式realloc并移交keys。Native no_client_auth不给可审计的输出硬上界；观察到183B不是生产cap。只读复核及独审另确认现有set_buffer_limit只管普通TLS缓冲、QUIC hs_queue绕过，不能补此硬界。当前Quinn/Bytes调用侧无法证明分配前额度，因此按停止规则不继续容器补丁或生产候选接线。推荐A：把实现范围扩到TLS provider，增加拒绝不消费queue/keys的精确预检/有界输出，并贯通Quinn fallible API与原硬账；保全部合同但增加依赖维护及验收。B：显式把CRYPTO输出改按独立配置/实例界，改变现硬预算覆盖，未获授权、不采用。C：维持当前NO-GO/charged Q和关闭guard。完整取舍/六处置仅在唯一计划，未自行选择B或推进A生产代码。

**本轮RED及精确收敛：** 最终源码核账曾发现仓外唯一未引用旧cold_tests文件，归档移除后同源重跑399/400：旧65 sparse测试实际0→8alloc、ranges/Q仍正确。前16次有界栈诊断将全部8次定位为真实TLS key update，不是tracing或ranges；首次key phase原随机10..1000，固定PN不固定epoch。测试现先用原force_key_update/PING/drive证真phase翻转及peer应用、只读test getter锁双端phase/limit且>130，再测同epoch原65/64/peak65/0alloc；不改生产阈值/期限/零断言、不忽略8。最新修后400/400；两个399/400原始RED与诊断源码均保留。这只限定cold ranges证据，不声称TLS轮换无分配。QUIC阶段oracle第11、证据镜像1均归原N1-NATIVE，不增父组。

**本轮执行：** root同版本rustls0.23.41的最终仓外Proto400/400、定向2/2及key-epoch修正定向1/1、根增长7/7，均exit0；无生产Rust行为变化，无Native联合运行/父组提升。Bytes17/Quinn12是前轮证据、本轮未重跑。独审仅签test characterization无P0/P1，完整库存/Root/Native仍NO-GO。raw `n1-metadata-provider-locked.log`、最终`n1-metadata-key-epoch-final-proto.log`、`n1-metadata-growth.log`，源/证据SHA及独审见统一结果。失败轮次不清零，QUIC阶段oracle10→11、证据镜像子类1，不新增父组。

**前一生产coherent阶段（2026-10-29）：** ACK/loss临时集合已由原History Slot内标记与同Credits线性guard替代，保PN顺序/ECN合并段数（含PN0）/原loss两遍及MTU/CC；真实加密ACK+lost PING处理1→0alloc。持久PendingAcks三space各cold65 Range布局在CID/TLS前取得、Retry保原owner；实际65稀疏PING接收6→0alloc、峰65终64。ColdResources准备事务行为保持移动到Connection冷库存Module。其余nonCID/STREAM metadata、Endpoint固定登记与crypto输出仍Open，不能由此声称完整库存编译闭合。

库存形状fault原Q后仍apply1 STREAM/无错，现typed Err经首包及普通包原通道在frame前停，故障包/已接管尾STREAM均0、first value保留、终态Q1；这是明确负向fault，不冒自然Full压力或整包rollback。原Connection遇真实peer stateless reset会覆盖先来的AppClose，现唯一setter保顺序first、reset仍同clock推进Drained。Error观察克隆仍持原RX backing：PairDrop230B→去一份仍230→最后0；不授权生产丢首错/提前退款，也未签跨Root occurrence ordinal。

该生产阶段实际全Proto398/398、Bytes17/17、真实Quinn12/12、增长7/7，scoped fmt/diff通过；独审本coherent纵切无剩余P0/P1，完整Native/Root仍NO-GO。raw n1-recovery-final-fixed-proto.log与n1-recovery-final-root.log；之前397/398以及全部RED如实保存于统一结果。19连接fixture按当前已计cold419840B/conn+1472Bendpoint定7978432B，两batch不变，不代全conn/RSS。

**后续强制门（先解除上述TLS接口可行性阻塞）：同owner非CID/STREAM metadata记录池完整纵切。** 须覆盖pending→sent→ACK/loss/Retry/0RTT的Box/Vec/HashSet/Deque/TinyVec、所有producer分配前reserve和六处置；不以更多恢复选择快照替代。随后Endpoint Meta/routes/reset固定登记、可控CRYPTO输出、source/defrag/typed、同Service双代Root预留/最终提交仍强制。Node16/startup retire已查、recovery峰值未证明前不猜vendor cap，禁止生产第二endpoint接线；完成原六处置→高RTT→默认并行→固定18联合→独审前，两guard关闭、未证charged Q。

失败累计hard-admission≥16、early-clean≥18、QUIC阶段oracle11、layout oracle3；新恢复ECN等价性1和编译Interface3是同现有父内子类，不加23父组。精确owner/不变量/原始证据只在唯一计划门禁增量。

预算化Bytes backing已具布局前扣、唯一header计费及最后引用退额。Quinn冷recv_buf与合法最大copy预检、接管前A1计数拒绝、rebind不扩大冷布局已有限验证。Node仍未接共同两代硬预算，不能把测试profile的保证外推到生产Node。

预算Quinn使用前扣forward普通/Rebind/Close槽，普通反向事件同步交原Endpoint owner，Drained用独占冷槽与固定ready索引。原map实际删除后ACK；真实Endpoint全19槽两批结算/复用及driver损失交错有有限证据。driver丢弃仍登记的连接结算前置Q；原回执失败不冒clean。共用槽owner已下移Proto，批量/单槽额度共用提交唤醒。

预算Proto原`endpoint_events`已接冷Slot与真实BatchPermit：连接/CID/TLS创建前取冷布局；实际I/O单slice界由Quinn传入，整包在首个状态修改前取`L+8`，first packet/timer/local address有前置额度，Drained另槽。已归Quinn的拒绝事件保留并结算旧项后重试一次，仍拒则Q/fail-stop，不能归入A1。server buffered accept对拒绝/Q优先原首错、原Drained回滚未发布map后Err。

**2026-10-29最新有限签收：** 当前所有生产普通事件入队点经独审核对，逐帧事件由编码字节、迁移由包头、握手固定事件由额外余量覆盖，`L+8`可作为当前单次datagram事件上界；新增emitter必须重审。原加密混合包394B包含NewCID1/Retire20/STREAM3，普通事件3，key-update两分枝Qfalse；确定性原Proto认证迁移路径亦通过。该事件纵切当时Proto368/368、Quinn12/12/growth7/7；当前全库结果见下段，独审事件边界无P0/P1。接收夹具已按生产入口前扣复制；它不是Native Service/真实多Flow/完整峰值或联合验收。

**最新CID冷库存有限签收（2026-10-29）：** 原Endpoint构造在CID/TLS前，同Bytes硬额先预扣`72×Node64=4608B`；线性F记录沿真实reply→pending→sent→loss/probe→ACK移动，不新增CID逐包Vec/Box。最后引用才释放冷Layout；中断/foreign authority进入sticky Q，已知库存销毁后仍保留1字节未知债。空Cold列表、push/append/receiver及0RTT discard共用同Pool权威检查；原driver端点失联时按已有State首错→Proto首错→generic顺序保留错误值。

原R退休/F仍sent→真实ACK释放的两ACK阶段通过，ctor按Event+F共同Layout拒绝且CID/map未修改，foreign reply零修改拒绝/Q通过。最新Proto374/374、本地真实Quinn12/12、增长治理7/7、scoped fmt/diff通过。仓外实际ConnectionDriver故障注入证明首错优先及driver回执/endpoint失败回执/Q1，只是负向组件证据，不是自然Service或Root JOIN。独审无可复现P0/P1，仅有限签收接口/所有权；**F六处置尚未完整闭合**，R map、通用sent/ACK等物理库存仍Open，不冒整硬峰值。

Native实际Node→quic默认Hashed lifetime=None，H恒空/N/A；通用Some历史257及seq6/prior11反例保留后续Open，未改配置/删除raw。本轮失败类别跨模式累计：R-BACKING-AUTHORITY第3轮已先结构审查并收敛共同权威；R-EARLY-CLEAN≥18；R-QUIC-TEST-CID-LIFECYCLE第10轮修共同ACK阶段；R-MAILBOX-LAYOUT-ORACLE第3轮。原始RED、修复与有限签收见唯一计划/统一结果。

**最新F压力有限签收：** 原加密合法轮换64条sent后free8，余下4条可轮换CID的原退休使4个具体新seq暂停/free4；每轮R真删除、恰补1F、原ACK数不增。Full下两个uni STREAM精确交付、首次CID不穿门；原loss deadline触发旧CID probe+2而库存不变，原peer ACK后free68/四seq转sent，最终peer新CID接收总数等于发送。独立fresh分枝在Full时原Close令peer ApplicationClosed、原双conn drained/map0，最后预算0/Qfalse。原harness有界推进，未改25ms/PTO。独审无P0/P1，仅签F局部压力/Close；实际覆盖4条余下合法退休，8控制槽是代码不变量上界，未逐槽实测。raw `n1-cid-full-real-final.log`；Proto374/374/growth7/7，本轮仅增测试与只读取证接缝，无新生产行为。

## 下一唯一执行门

**当前先停在TLS输出可行性取舍；下列已批准A是sent-history控制耗尽的合同，不是TLS provider扩写决定。** 不因前轮sent准入已获授权就声称完整库存可实施，最新结构停止结论优先。

用户于2026-10-29明确选择“批准A：控制额度耗尽时连接级失败”。旧合同选择阻塞已解除，恢复单Writer实施；不再等待A/B选择。普通sent额度Full只暂停新DATA；控制额度将耗尽时，在新包编码/包号/相关发送状态变更前登记唯一typed容量失败，由原Root/Node按原已固定期限实际stop/drain，未证资源继续charged Q。这个特定例外允许同QUIC conn全部Flow受影响，其他连接/Node不得连坐；不是新增ACK超时，不改原5/15/PTO，旧双witness/候选无DATA及其它禁令保持。B不采用。授权不是运行时验收，N1-NATIVE仍Open，两guard关闭，原父组/固定18/失败轮次不变。

下一唯一缺口仍为同Conn packet/control/ACK库存及原Endpoint登记的完整事务（见唯一计划顶部全范围表）。原样移动/Legacy Characterize、冷Slot/control余量、无heap的ACK/loss选择与cold65已有限通过，下一须闭非CID/STREAM记录池及其余route登记，所有实际发包入口在PN/crypto/frame修改前取额；普通Full需让ACK/PTO/Close继续，实际控制额度耗尽才走获批容量terminal。成功证据是实际cap前两个同conn Flow、ACK返回恢复、实际耗尽时唯一首错和原清理/Q、其它conn继续；失败按唯一计划跨模式累计，结构门保持。若有未覆盖分配或发送旁路，停止该接线审共同owner，不局部退款。

此边界完整可编译、六处置及独审通过后，仍须闭合原Source/defrag临时新旧库存、typed队列与共同预算，再完成Service/Node双代OverlapReservation和Node/UDP/health/Device cut receipt的同Root最终提交。后续均为本目标强制义务；不得以重分类移除。普通Full必须保控制进路；仅获批的控制history耗尽例外允许该conn失败，不能扩大为其它Node/连接连坐。

之后才允许一枚候选的真实旧ConnectionDrain Pending/同代stable-id、候选AUTH且无DATA、旧双witness后唯一发布，并按原顺序验证六处置/共同接缝→高RTT串行→原full matrix默认并行→固定18联合→独立签收。必要资源/提交控制权无法闭合时停止局部扩展，提交具体合同取舍。

## 冻结合同与停止线

单Writer、独立Reviewer；同类两轮局部修复上限，第二轮共同接缝，第三次先结构收敛；换测试/模式不清零，轮次以唯一计划为准。原5/15/PTO、首错、无法证明释放则charged Q、增长门及全部禁令保持。

用户已批准：旧Root结清、真cut receipt、旧close后，一枚未发布且无relay DATA候选transport可与旧真drain重叠；旧endpoint/driver双回执仍先于唯一DATA发布和Device resume。可控backing/owned queue同硬字节额度，其余endpoint/连接/任务/OS按独立实例和配置上限预留核账。A1只允许Full时计数拒绝尚未进入Quinn Proto/Root所有权的未入账UDP包，不允许丢弃已接管资源。

原full matrix的successor5从retire15返回后起；TCP/UDP后续各有原观察段，不能称共享5秒覆盖全部DATA。重叠AUTH不能独自解决旧drain耗时；B820/B830/B832反例保留，任意RTT/PTO无固定时间恢复保证，到期失败仍Q，不扩窗或改断言。

**仍NO-GO生产Node预算/第二候选。** 两Native guard在完整原Root COMMIT/JOIN、物理账及actual join联合证明前关闭。公开TUN仍走legacy；不得运行macOS TUN、VPS、WAN、吞吐或M2；本阶段不commit/push，不把注入/夹具冒自然生产路径，不局部退款或绕receipt。

## 最小证据入口

- [唯一计划](n1-f1-implementation.md)：固定18清单、唯一剩余工作与失败轮次。
- [统一结果](n1-resume-20261017-evidence/b560-result.md)：最新冷事件纵切、限制、独审与raw索引；raw根为 `/Users/liushan/.codex/visualizations/2026/10/26/01a17639-85fe-71e1-8ea8-8af5b0671fb4/`。
- 原Root/物理边界需溯源时按结果索引读取B74/B138/B243/B529；不默认加载历史。
- 本次压缩前的STATUS原文保存在 [导航快照](../archive/project-memory/navigation-20261029-cold-event-burst/STATUS.md)，不作独立当前状态。

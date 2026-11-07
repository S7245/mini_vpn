## 2026-11-06 B5 探针 49 分钟静默挂起：测试 println 内两次 `ledger()` 自死锁
`println!("{:?} {}", x.ledger().a, x.ledger().b)`：首个 MutexGuard 临时值活到语句末，第二次 `lock()` 阻塞 current_thread 唯一线程，tokio 超时永不触发（CPU 0.1s、睡眠）。`| grep | head` 缓冲使输出全空。处置：`sample <pid>` 取栈定位（__psynch_mutexwait@SessionControl::ledger），非产品挂起。今后：每语句只取一次 `ledger()`（先绑定）；探针一律 scratch `guard.sh SECONDS OUT cmd…`（墙钟看门狗+超时 sample+直写文件），不经 grep|head 管道运行长测试。

## 2026-11-05 Proto fork 测试间歇失败：根因为每连接随机 RNG（D20 已定位，残余 2 个）
`n1-a1-gate-20261105-proto` 首跑 `send_buffer_inventory::key_loss_and_stop_sending_full_preserves_packet_and_recovers:165`（server `force_key_update` 后 key phase 未变）失败；本轮零 quinn-proto 改动，单独 30/30，全量复跑 10 次中 1 次为 D16 已知 SameStreamFrames。今后 Proto 门单次失败先 `--exact` 隔离与全量复跑定性，原始日志保留，不改期限/额度。D20：不是并行负载，是 SysRng 种子的随机初始 key 寿命（10..1000）与随机 PN 跳号；"单独 30/30"只是低概率（0.5–4%）样本不足——间歇测试要用千次级循环量化并打印随机输入（key_phase_size）找判别条件。D21：收敛为 admission fixture 单一固定 `rng_seed` helper（77 处），残余两测试 1500 次 0 败、全量 20/20；不要逐测试补第三处同类补丁。

## 2026-11-05 A2 测量夹具的三个假象
(1) 同进程连续场景 baseline 被前一场景迟到释放污染（Reorder peak 0.79MB vs meter 39.5MB）→ 每场景独立进程。(2) TxStorm 用 `write_chunk(Bytes::from(vec))` 把未计费 Bytes 塞进计费连接 → 连接以 "stream physical inventory authority lost" 内部错误关闭；生产上行走 `poll_write(&[u8])` 计费复制，夹具改 `write`。(3) 最大尺寸 datagram 用 `send_datagram_wait` 在发送端永久阻塞（与 ACK 同包放不下）→ 改非阻塞 `send_datagram`+32B 余量。均为夹具错误，非生产反例；原始日志在 scratch a2-k-run1/2/3。

## 2026-11-04 多早期报恢复失败的共同NEW_CONNECTION_ID编码根因
n1-coalesced-stock-qualified-matrix7/9、diagnosis2/4均原peer保FRAME_ENCODING_ERROR NEW_CONNECTION_ID malformed；旧CID丢失后，writer用新retire_prior_to超旧sequence。原编码min修后同矩阵9/9，原peer/其他流/期限不改。boundary模块路径、matrix不存在FrameType方法是测试编译错，单列保raw。后续第4/8实际TLS输出错误/展开仍精确保prefix/非零Q，完整685+Doc3/653绿不冒历史Direct旧首错已归因。

## 2026-11-04 CID限额后置与跨space MTU错删
n1-cid-loss-actual-red真实ACK/loss已提交后才报CID限额；同owner准备精确前置后普通/Full均保旧库存。最初real-red为fixture借用编译错、qualified-red为stored/pending tuple oracle错，不算生产反例。自审mtu-space-red差分实证原Handshake6未丢而Data6被错删；原loss限定Data空间后qualified-proto681+Doc3及联合653绿。保全部raw，不以首final-proto绿替代新反例修复。旧Direct/SameStreamFrames未知包形仍单列。

## 2026-11-04 未通告新ID回退旧路径与计数oracle
n1-stream-limit-qualified-red真加密RESET先退休、同包未授新ID被错误接受；资格fallback移除后普通/Full拒绝且保原库存，合法实际通告后继同Box通过。real-red编译借用与boundaries遗漏peer应用RESET均为夹具失败保留。首全Proto674/675为累计lost=2；隔离新基线仅0→1，不冒旧额外包已归因。改共用fixture严格核gap@6/tail@13及skip未重传，完整675+Doc3及原联合653绿。预算/期限/交付不放宽，原raw/旧Direct失败保留。

## 2026-11-04 RESET原路径部分提交与测试观察点
n1-reset-lifecycle-real-red真实RESET+STREAM在Full前改变协议；同owner生命周期/容量准备后转绿。first-green为ShouldTransmit接口/测试match类型编译错；boundaries为测试helper可见性编译错；boundaries-qualified671/672为旧foreign空FIN测试要求frame计数增长，和新提交前拒绝冲突。改核真实wire，保双authority Q/原库存，完整673+Doc3及原联合653绿。上述失败不抹除历史Direct652/653；同包刚授予新ID仍旧路径，A未闭。原raw全部保留，详b560顶部。

2026-11-04 STOP/CID/path真实mixed Full部分提交已修；两次CID fixture与首次path fixture只错在假定单包，actual-mixed-red/path-qualified-red才是生产反例。first-green仅InvalidFrame map_err遗漏，原retire6提取先绿，最终668+Doc3/97/653同源。STOP已覆盖后原key/ACK/loss/STOP不能继续要求failstop，改验证同wire无选择/无key推进、真恢复与另一流；unknownQ仍由原provider故障验收。CID ACK/loss+MAX_PENDING、RESET退休复用、旧Direct归因/独审仍Open。

2026-11-04 首次Data混包实际356B，另有1326B MTU probe；不能假定仅一个outbound或用Debug Type(09)识别STREAM。n1-first-crypto-actual-wire-red才构成Full后ACK/Handshake keys部分提交的生产反例；前两个fixture失败保留。共同prepare后660+Doc3/97/653通过；fault-check仅CidState Debug测试探针编译错，改私有精确字段。旧Direct未知包形与独审拦截仍单列，不由新GREEN归因。

2026-11-04 coalesced真实后项Full：n1-coalesced-actual-pto-red 0/1，前Handshake包已改两space PN/ACK、认证计数/timers/path。修同原Connection准备对象/数据报共同提交与TLS idle实物屏障，5实际边界/651+Doc3/97/653同源通过，非完整A。保初始可见性重复/借用编译错误与扣住Initial ACK的PTO fixture失败，后者不计生产反例；历史Direct未知包形及独审拦截仍单列。

2026-11-04 原控制帧与STREAM共同事务反例：真实MAX_STREAMS先发布信用/事件，后STREAM Full产生部分提交（n1-controls-real-red 0/1）。修复共同prepare/commit，而非增加事件余量；首次Send须实际预持，后项Full全部取消。n1-controls-complete-proto646+Doc3/原联合653同源通过，历史Direct未知包形未由此归因。测试边界首轮仅assert_wire私有可见性编译失败，pub(super)后140准入通过；独审平台拦截仍独立记录，无新结论。

2026-11-04 materialization真实0/1→1/1；boundaries29/31是新测试未FIN却要求EOF，真实finish后2/3；剩余缓存已有Heap/事件容量，在Full合法成功，不能改生产强制拒包。改测实际有余量成功与真实事件槽Full拒绝后31/31；新unit的VarInt字面量i32编译错误修后完整640+Doc3/653通过。全部raw保留，额度/期限未改，首包不是已有Open的替代证据。

2026-11-04 key真实0/3反例先变phase/PN/timer再Full；实现后原key-ACK断连断言按覆盖范围分恢复与真实MAX_STREAMS fallback，保原selection/首错。新增fixture两次缺实际frame：默认receive_window已MAX，MAX_STREAMS diff需>new_count/8，须按原规则算并核wire，不看调用名。活跃流Q有原保留库存，不能套握手Q1；用原retained_layouts独立求和核精确999等实物，不改生产Drop。全部red/28of30/35of38/36of38与最终636/653见b560-result。

2026-11-03 n1-datagram-proto-complete 615/626的11失败同因事件Queue Drop见global Q就保实物，多留384/768B破原Q1/精确库存。分离准入与最终物理核验后原断言不改，新增失证保留tracer，627通过。真实mixed RED、可见性/借用编译失败、空slice helper Allocation全保；Bytes::new不免descriptor/事件计费。完整653通过不代key/newstream/coalesced或独审。

2026-11-03 多STREAM轮：原联合首次652/653，Direct收到5375714B后超时，实际释放2684961B/nonQ但peer已receive inventory admission refused；诊断focused/joint均未复现，不能拿后绿抹前红。改确定性包形矩阵，IMMEDIATE_ACK+STREAM 0/2证先写立即ACK后Full，已合入事务；仍不能冒该次未知包形根因已确证。FIN20→15旧接受也由RED修复。测试前置须核Free/Open、真实key phase及原KeyDiscard；不要机械替换enum分支或误要求没有uplink-loss的场景必有loss。全部raw、最后616/653验收和剩余义务见b560-result顶部。

2026-11-03 ACK事务首版：保守loss上界为ACK已移出History的记录多备重传树，原联合652/653、Byte仅4093peer/WouldBlock/Reset、原期限618784208Q；Permit峰多654560B。先用真实ACK需求(2,1)≠(2,0)锁根因，再按首次有效ACK顺序收敛，最终双F4096峰恢复及653通过，未调额/延期。Proto598/600的旧“纯ACK Full必须断连”断言按新合同分为合格拒绝/真恢复与实际key-update原failstop，两种义务都保。保所有编译/RED原件；runner必须检查commands child exit0后才启动贵联合，wrapper0不可靠。exact n1-ack-root-*归b560-result顶部。

2026-11-03 原Connection整包轮：保留原联合652/653、651/653、651/653；临时wire诊断实测ACK1+STREAM1仍receive inventory admission refused，最后653通过不证明共同根因已修。初始ordered fixture有Heap余量，不能冒Full；ACK单PING未达原阈值，必须观察真实两PING后的ACK wire。恢复测试直接Drop活跃Pair后864B非零，“仅密文持有”猜测被否证，改既有正常peer关闭后0/nonQ，不凭猜测给864归owner。E0596/E0503和所有RED均保。独立packet_review被平台自动审核拦截，独审缺失单列，自审不替代。exact归b560-result顶部。

2026-11-03 原接收边界保全部RED：缺尾槽读账先改、Free STOP先物化、空Heap下孤立AVL、丢root被DATA重绑、offset mismatch掩盖缺尾槽均由实际反例关闭。空FIN测试最初只驱动Endpoint未处理Connection事件，不算触发生产反例；E0061/0-of-2/582-of-583原件均保，补原事件队列和actual frame计数后才签。原Service645/653暴露7个默认Generic冷故障fixture提前被新guard拒绝，只显式选择合格Direct，原注入/额度/期限/断言不改。最终652/653仍Direct+Permit failstop，前轮Ordered反例不得用另一轮绿覆盖；full恢复未签。exact见结果顶部/n1-receive-pages manifest。

2026-11-03 Leased联合638/642：exact冷成本漏原clock16B、32/512旧observer溢出、并行Permit F4096 Node尾1632362B/nonQ/只余324ms均保RED。真正修复后同648通过，不拼focused59。observer修复首两compile101由重复tests声明/误覆盖入口及错误snapshot路径引起；按运行前精确hash恢复原tests文件再增child，禁止未查存在就覆盖。原14/21中6旧缺key仍归C，另1wire序号重复不能先断内容一致，实际逐byte/cut索引核验后才绿。原provider回调3次审计RED→两实际重入各1次，其他dirty/首错/期限保留。

2026-11-03 容量测试oracle：candidate仅AUTH未data应StartupResourcesSettled，旧Node drain须只停原Session不可误停全Service。n1-capacity-overlap-first0/1保留。runner独占日志遇旧n1-capacity-final-governance.log后拒绝写入，前joint571通过不代表治理已跑；新partition前缀完整同源后才签，旧证据不覆盖。

2026-11-03 最终release采样必须命中实际释放阶段：late-profile本轮16/17、73469425B/Q，首次证实全Root审计主导；不同轮独立GREEN与默认并行RED不能拼接。OpenOwner前冻结扩大旧特征40/33，33均旧证书缺失（原tuic.rs1487），保原C义务、不补旧key。原provider重复审计新反例14/15，clean后断言6次；先封所有写入口再在同同步Turn复用，不能裸revision绕过whole replacement/poison。

2026-11-03 F4096停止：同调用census融合独立GREEN余129ms后默认联合563/1；context-first独立GREEN余651ms后默认联合564/1，均Node尾1632362B/nonQ而非Root/App未清。实际Root与Node真实PTO尾须同一次原15秒通过，不能拼独立GREEN或归因采样/耗时差异；失败raw n1-shutdown-census-final-*、n1-census-bidirectional-final-*保全部。

2026-11-03 n1-main-stale-tick-actual-red先真实budget0/nonQ后stale13断言RED；Skip后focused21过，但n1-main-tick-skip-real-capacity仍Node尾0/1、1632362B/nonQ/firstNone，deadline202695.082837541。不能拼接不同轮JOIN/release/peer或把Root/App0当完整物理结算；保所有raw。

2026-11-03 完整关闭：原C/no-F actual bare rearm的empty clear设置terminal，下一Native capture以Inventory拒绝；n1-full-capacity-stop-capture-exact保实际坏ctx4224，修复后实际RST反例与50项定向绿。F4096原15秒仍分别418258721Q/483539553Q，首错None，不能把earlyQ修复当停止完成。n1-stop-census-borrow-characterization 91/92揭700字符护栏已过期，改精确同步区段后92/92；全部raw保留。

2026-11-03 完整容量反例：原真实F4096 fixture须匹配Device原16384槽；局部64槽冷拒不代目标容量。对齐后两轮在约3456/3705 F超原波观察窗，前者475493797Q、后者535772929Q在原15秒清理保留。后者失败前718374758/nonQ/无首错，不能误诊Full。实际运行采样指向Main capture重复全量核验；关闭采样失败不代关闭根因。全部n1-full-capacity-* raw保留，固定quota/并发/期限未调。

## 2026-11-03 SendBuffer真实caller与完整回归反例

原145B Full及3类错资格、empty FIN Q→Closed、staged CONNECT priority丢Admission与已close真实half不退休、encrypted loss外层ACK selection未结算均保RED。原release锁内failure查询3次/Drop、回调后outer epoch/账本变化及前置Q仍query、Soft误停Session由同Flow完整资格/锁外托管事务修复，保原Box、first和3秒早期限；native closed的未accept远端key不能计opened_send，wait_idle也不代actualdriver drain。全部编译/RED归n1-send-buffer-*、n1-flow-release-*、n1-native-closed-*。

完整Quinn两次95通过/2失败：reply_poison旧Q=1未计新实际metadata；另一个global owned_event拒绝计数来自control，过早释放票据使原mailbox-full先交付，不能断言首DATA必丢。现用实际ACK屏障＋独立Layout/token冷核及原meter三零；同原2秒最多两原帧仍须exact after-marker。97通过/3原ignored、Proto570、root519与7/20最终同epoch；n1-quinn-full-*诊断保留，不以隔离1/1抹并行RED。

2026-11-03 原uplink表：保原6144B退款0、早期6216B各归还/终态/parent反例；加入原Packet后6248B仍发现AUTH后未data正常stop将Session3秒改15秒。真实Endpoint已clean0/nonQ再断言RED，避免清理假象。共享parent turn锁内线性化pending Q→Ready，实际Arc Drop后仅重升原Weak，不能复用旧Arc。最终506/506、7/20/check/diff及有限独审通过，raw n1-uplink-custody-*全部保留；旧缺key仍C，不扩大SendBuffer签收。

## 2026-11-03 原Main阻塞IO使deadline检查不可达

Heartbeat前扣修复后联合465/2暴露旧RESET回归。hold ACK真实TUN队列64/64，原flush等待使census停2101/control30，原期限+5仍SessionnonQ；不是“已Q仅漏上报”。保serial与两诊断raw，不退Heartbeat或drain测试队列掩盖。期限观察必须在仍可poll的原IO/控制边界，Q保实际Pending future/capture/selected/ACK，新增等待字段独立托管和布局另核。

## 2026-11-03 DATAGRAM调用覆盖与runner磁盘失败

原Service UDP Full编码反例虽绿，原Heartbeat/TCP pool probe仍裸Vec→同Native Conn；新增实际Node两条0/2（Full也Q），说明包路径绿不能代完整调用覆盖。修复沿sameConn原meter、锁外前扣2B、原runtime typed分类，原Transport错误与Probe SendFailed/期限不变。错误链测试14/15是fixture漏Node owner attach，不弱化node_stopping断言；首编译漏harness/private可见性同保。联合n1-datagram-funded-integration-current在ENOSPC时stdout panic且最终report创建失败，不能取局部green计通过；保原log/source/runner-failure，按精确hash journal仅移32旧rcgu.o，再同源460/460。原binary/raw/dirty不清理。

## 2026-11-03 原协议发送copy与descriptor不能由上层typed身份代证

容量独审定位hard UDP原编码Vec与Quinn stream ByteSlice.to_owned、SendBuffer段及ACK/loss BTree ranges未前扣；真实UDP满额33554432仍Vec43/drop0且peer交付，末0nonQ后RED。Datagram字节界也不界0B消息数。先闭实际原owner完整准入/处置再扩大Leased；旧449只签其已列上层范围。raw n1-capacity-udp-uplink-real-full-red及n1-datagram-native-*。测试活Conn直接Drop的Q1不可当clean oracle；先真实close/drain再断言准入。

## 2026-11-03 闲池可用不等于网络预算可用

一次精确冷成本补偿后Full仍RED，且本次无首错/nonQ，不能套用旧Proto heap failstop根因。超时实际stock funded7/cap3211264但views0、原grant written0；普通末loan只返池可用性，Source阻塞Quinn并未显式退实物，Host继续无RX预算。证据n1-uplink-full-fixed-cold-cost及full-stock-diagnostic-run；禁止继续调额或由计数零伪退款。现由原Source/stock独占Pending退休事务修复，最终1210源码449/449及独审有限PASS；全部旧RED保留，不签任意预算恢复。

## 2026-11-03 上行精确计费暴露Proto旧新heap峰值

原32MiB上行Full已实际保socket，不能据此签8MiB下行Full。新1024槽lane冷费移位后，Proto Assembler256→512在剩余额不足新layout时按原已认证failstop终止；有的epoch先于RXFull，有的在ACK恢复期间。须分别保原raw、具体stage/布局和first；不能改成drop/retry或按peak反复调测试额度。证据n1-uplink-lane-joint、three-isolated、full-proto-diagnostic；原failstop语义保持。仅按实测双lane冷成本补偿固定100304B，随后真实闲池退休修复使最终原Full/ACK绿；不宣称旧8MiB本身已修复或任意预算可恢复。

2026-11-03 初始上行保typed-wire/escrow编译失败及两个错误传播缺口；私有generation直接读取E0616与Service测试直接读私有账本E0603/E0616改为原owner测试观察。原Full rollback清理2秒超时源于fixture漏真实poll/flush，不延期限。nonFull parent3秒变now+15、Q Flow deadlineNone两个真实RED后统一min修复；最终430PASS/1原established Full RED及7/20/check/diff原件保n1-uplink-*，不称联合全绿。

2026-11-03 容量审查锁定真实hard上行P1：budget33554432 Full仍从原socket消费1460B并生成原Vec1460，随后原QUIC/5JOIN/末0nonQ才断言RED；raw n1-capacity-actual-uplink-full-red。首轮move过滤命中此RED而35/36，非新增回归；before-filtered与after35通过。未修复前不签完整容量或扩大接线。

2026-11-02 Source gap保两确定性RED、guard3/4、cargo多filter CLI拒绝和联合378/379。guard失败是将实际空FIFO352B Drop误计为Q库存；按独立实际Layout另核，不改生产退款。旧capacity测试等received4超时是读前准入改变：真实JOIN后received0+polls0验证原输入未取，保原1s/WouldBlock/单ordinal/4JOIN/末0。最终379/7/20/check/diff同1202源通过并有限独审，全部raw n1-source-gap-*保留。

2026-11-02 RX/Session历史开发失败（后已有限签，见结果顶部）：原8MiB Full在RX曾两绿后，最新n1-device-rx-session-union-before与full-assembler-diagnostic都RED；实际gap tree4节点/5676B，next2761331，incoming2769845+1419，assembler_push BudgetFull，首TcpOpen/WouldBlock、原15与4914673Q保留，不可冒RX失败或改gap原failstop。原Actor STOP/PAUSE doubleDrop SIGABRT保；逐字段托管后2/2。原IO alias越deadline不quiet真实RED后同原5ms控制节拍修；22/22只属当时epoch。新真实Main/Observer/Tail重复1、EIO5丢raw、Direct未进Core三RED与readonlyQ伪IO RED均保，正在同原错误传播事务修复。E0603/E0716/E0432/E0061全部编译原件保留，不计测试执行。

2026-11-02 Source Byte：保222/238、15/16、Main两None-key RED、gap[8,8] RED、旧fixture未bind clock Invalid、两helper编译错及单poll≠registered的lastalias1/2。最终250/7/20/check/diff绿；原同epoch8MiB Full仍0/1，首错DeviceRead/WouldBlock，deadline148386.444278666、charged4846046Q、两Flow各4JOIN/F1保留；supplement767e567e…eeafe5，不用部分绿掩盖恢复义务。

2026-11-02 保n1-source-output-finish-q-runtime-red与两次237/238：Q finish原Option丢失、None未Q均已修；Chunk尾账被App末次消费、Direct4JOIN早于Retiring均在原5s观察界修。全停App两次RESET RED与DATA先terminal/FIFO discarded0合同互阻，不据此改生产fatal；n1-source-admitted-output-lifecycle238/238，原8MiBFull未绿。

2026-11-02 Source控制：真实watch Sender析构锁内RED、hard unwind未发布budget Q、Soft factory Event/deadline回归均保并修。n1-source-flow-unwind原21/56中33项旧key失败仍C，另2项Soft回归已闭；classification-v2更正首次自动分类不覆盖原件。175/177暴露unmetered mismatch fixture及JOIN≠provider退休；真实authority与原时间界分事实复验后177绿。全raw/编译/SIGABRT保留，详见统一结果顶部与1cf65bae…54001有限审文，真实FullACK未闭。

2026-11-02 Source当前修复：原zero-poll SIGABRT6由逐字段托管转绿，真实opens内query/Drop RED由统一Native disposition转绿；扩大回归158/163暴露Q禁wake阻止Source实际JOIN及RESET终态事实滞后。新Q-cancel tracer wake计数0/1为RED；6/7增长失败为test index306>300。保所有原件，正在同原控制turn修复；未签当前Source。

2026-11-02 coldturn：原锁内opaque probe、错误epoch在析构后才校验、Full被后Drop panic抢首错均有真实RED；typed capture前已有Q在15秒内被重复构造3001条egress IO，修后egress0/仅后原panic1。后半程ACK遗漏typed原因与callback误改BrokenPipe均由回归抓获。lost token实际对象保住但原期限晚41541ns，不能靠正Q通过；原min严格断言RED后补同owner锁外quiet与回填min。全部raw不覆盖，最终120/7/20/check通过并有限独审，FullACK仍RED。

当前资源/验收教训：n1-source-single-callback-verified编译因ENOSPC失败，连commands JSON也半写，均原样保留；只清本仓mini_vpn incremental，之后CARGO_INCREMENTAL=0，未删deps/raw。Ready纯移62→61/62出现原同deadline外层timeout后phase Stopping!=Q；隔离1/1及同epoch62/62不能抹掉此完整A/C时序义务，不改断言或延15秒。

当前源码身份教训：不能按任意路径段target排除Rust；src/owned_upstream/target是生产模块。n1-progressive-read-final仅实际1165项索引覆盖，遗漏8源/Cargo.toml另经review与前epoch交叉核，不补造历史冻结身份。后续只排除仓库根构建产物。

# Current failure memory

2026-11-01 Source纯移动冷test原expect sentinel1已过时：actual exact before也末864，整NativeWaiters在Q保冷布局。先原生产不动，owner只读布局getter＋冷前后payload三0/精确费用，保5fault/IO17/order19；前后GREEN后才签移动。n1-source-reader-before-cold-{red,green}保全，不因新module路径归咎移动或改成>0。

2026-11-01 n1-device-control-*保原满额RED、reuse未清零RED、cold零长alias RED。先实际未清零/marker反例，再原地fill及cold实物唯一核验；不调预算。最终Device93/hard62/治理27/check0有限独审通过，原A1 Full仍RED。未pin中间源码不倒填为当前epoch；最终snapshot/index全冻结。

2026-11-01 n1-device-control-backing-full-*：原Egress.token每次临时分配MTU+PI，真实32MiB Full/已staged40B/TX0仍receive None；同帧仅实际DATA Drop后可读。最终RED前先真实shutdown/receipt/Drop并验0/nonQ，不能由故意panic制造owner-drop Q。new capacity测试不代原双Flow A1反例，新增合同候选需用户明确修订；同epochmeasurement3过/该1 RED/治理27/check0。


2026-11-01 stock/FIFO：n1-output-stock-*保原API/编译失败、lastalias未唤醒、late return误Q、旧入口异属loan、quiet owner/queue loss、opaque首Drop、退休重复acquisition、缺record/identity假clean等真实RED。v1有限审漏跨budget拒绝，源Q未保证receiver及pending各quiet；真实反例推翻后作私有ledger能力收敛，纯move75后v2全77/原62/39独审重签，旧审文不覆盖。冷Full测试最初S8192>budget4096得TooLarge，改S1024才真测部分冷备回滚；参数入口错误不算执行。两机械rename E0599及E0616均保，不凭green文件名判断。全链Full仍RED，不能签A1。

2026-11-01 A1真实Full反例：原8MiB双Root在RX已计数拒绝时，assembler typed输出copy已BudgetFull并quiet报WouldBlock/remote_read_failed，global nonQ不能推断reader仍活，不能直接归ACK自锁。按完整FlowIdentity分别核queue/terminal、逐caller诊断后才选结构修复；raw n1-a1-backing-pressure-*全部保。原deadline+5观察Q不改字段，末charged6245005Q、2×4JOIN/F1无假退款。

2026-11-01 A1 mailbox tracer：真实后续datagram输出仍持原Bytes时budget113，属测试末引用未退而非生产泄漏。先记录内容校验、Drop原输出再验双driver/budget0；保n1-a1-rejection-cause-mailbox-run.log原RED，不为测0强制退款。

- n1-hard-partial-*保冷128/64拒绝、pending猜测否证、单flush/单Refund与leased单域oracle RED、E0432/E0425、全域初次及joint61/62全部原件；真正尾账在global ring，非pending或泄漏。完整实际七域与wire/refund模型后partial1/1+prefix2/2；joint唯一hardChunk100ms未见Q，原clock相同/Flow更早证无重启，只按既有5秒观察界修。verified62/shared29/7+20/check/diff全绿独审，生产未改，无新SIGABRT。

- hard ordered raw n1-hard-ordered-*：classifier漏caller E0425、focused Event导出E0432保留。Permit first2/4与runtime1/3都由旧test每Admit新ordinal误拒原Native cap1扩展；完整真实receipt与diagnostic源保全。strict live extension+其余总账自洽deadcap拒绝后green6/6、最终58/治理7+20/diff独核通过，生产ledger无改，不重复加定向计数。

- hard真实RESET n1-hard-reset-*：Direct0/3新增deadline oracle过早采None，终态Some(min)才是原合同；Chunk0/2是裸snapshot主动reject_raw，非生产队列故障。passive2/2后用原Roles settled/全收据强化释放observer，不凭Node让出，settled2/2/最终48与治理通过。原RED不删，未改生产或源first判据。

- hard loaded raw n1-hard-loaded-*：Direct首轮3/1，原Flow比parent早约22ms到期Q后Main派生first并非底层IO；因果probe匹配并逐字恢复，精确分类后4通过。carrier-red是真同returned原载体additional重复，contained去重后focused1/egress10过。TLS纯移动28/29为joined8早于双provider settled，保原5秒同时等两witness后29。E0425编译与全部RED/诊断原件保留，最终43/治理7+20/check/diff有限独审。

- Direct hard raw n1-hard-direct-*：首次17编译缺父上下文；normal/cap1/1的cap标签预期来自unmetered路径。cap-probes证remaining0/4MiB先Q→Main entry_q=true拒admission，非独立Main库存P1；两生产probe逐字恢复，before/probe/restore hash全保。改读原Shared真实terminal后cap1与全29/治理通过，无生产修复/无原capIO承诺。

- hard Service新增测试保n1-hard-service-*：factory E0271/借字段E0507是编译问题；terminal0/4含未命中原wait与未到DevicePending；trigger2/2和diagnostic揭示3秒必Q错误预期；join-proof1/1因UIphase瞬时Running而非JOIN。已审原共同fatal/Control权后修测试，未改生产fatal或延原deadline；verified26/26与治理通过，全部原件不删。

- 原UDP组装（2026-11-01）：n1-udp-composition-*保全部原件。真实EOF/unused-stock、Service预算残留、typed Device wrapper漏代理、owner-loss（保observerArc更强反例）、frozen EOF、Session漏首错、Q派生重复IO及Source proof首次panic重复IO均有RED→GREEN。taskset-red是exit101/两原Box被析构，非SIGABRT；first-runtime的9缺旧key失败归C；disposition-service误筛0不计；compile族另记。最终29源75raw有限独审，未弱化budget0/nonQ/早期限/非零Q，旧secret未造。

- UDP callback：真实drop-red 3/1与entry-red 6/2分别证Work Drop后假成功、零poll参数裸Drop、Q重复写同失败位；complete的E0609只属测试字段误用。另独审查获cold局部stock先于Box退款及&Lease并发Work。原raw均保留n1-udp-callback-*，集中修同owner同步准入/独占/物理先销毁，未改生产UDP。

- Native fragment曾在remove后发现重复index、空输出漏descriptor，以及replacement/resize/oversize/LRU清掉foreign原backing。三组真实RED完整保留为 n1-udp-native-fragment-{commit,empty,authority}-red.log；修复集中于原selected事务预检，不能只给完成copy加meter门。

2026-11-01 双quiet漏报：只保ownerDrop Box/Q会漏活parent的stop/期限，逆序也会漏活Node。真实live-parent与reverse各0/1已锁定，固定三步释放＋独立ordinal/连续primary水位闭合；clock10/实际Session2及原回归独审通过。raw n1-udp-dual-clock-*保所有编译/运行RED；Node生产UDP接线仍Open。

2026-11-01 Shared context P1：真实单Drop越过Observer.finish，双字段Drop子SIGABRT6；只测消息销毁漏了lane级上下文。已以同Core MD/静态拆解、cold迟交付及原observer失败保留修复；shared29及原回归有限独审，raw n1-shared-channel-context-*，全部原件保留。

2026-11-01 Native组装raw n1-global-core-*：actual RelayEvent→permit→Notify子SIGABRT6；receiver退休Drop触Q后仍逸出value、正常close抢Producer误Q、recv取消留waiter均是真RED。代理分捕获/held custody/独立closing/取消guard已定向通过；新增cold门E0609和旧oracle失败单独保留。另有foreign/raw permit混入真子SIGABRT6；以原queue meter＋真实Native Root资格拒绝并保槽闭合。最后旧roles构造/ReportedIo oracle分别17/20、19/20保留，最终roles20与全固定组/治理通过、39源46raw有限独审；不冒完整Native完成。

2026-11-01 Global组件：Granted后Q仍发Unit、重入grant wake峰2、未绑定clock可冷建、stale tail单次push假Full四真实RED已保并关闭；E0432/E0507/E0373及Bytes backing预算oracle失败分别记录，不把concurrent-green文件名当通过。实际credit9/ring5/clock3/Service2/治理7+20有限签，生产Native组装仍Open。本次接续7源/7快照/23raw匹配，但两个临时scratch目录/指针已缺失；保历史manifest，不重造指针冒当前复验。

2026-11-01 Global source：原source已失败/0JOIN却first None、Root两carrier Drop真实SIGABRT6、Q后shutdown二次poll、opens锁内原IO Drop均保RED；统一source quiet发布、Root库存托管、每次IO门及唯一在飞token锁外处置关闭。E0509/E0433/E0624、并行CA目录碰撞、无hard budget的Q oracle、错target101及三个0筛选保原件且不冒功能验收；正确settlement7/roles20及治理7+20另有raw。最终42源码/70raw/2pins有限独审，global/A/B/C仍Open。

2026-11-01 global原Semaphore.close的消费式wake可让wake panic与末Arc Drop panic直接SIGABRT；外层catch不足。raw n1-global-lane-credit-close-sigabrt.log子signal6，parent仅反例观察。Native需在同owner分开wake_by_ref与Drop并保首错/Q，不能把逻辑credit库的opaque callback视作无失败。

2026-11-01 N1真实Service链发现initial_owner早于F4096选择：App已Established但flow=None，两模式原5秒发布超时且15秒stop未完。select前移后真实Chunk/Permit各8192B、五JOIN及末预算0通过；raw n1-native-complete-service-*。缺uuid/Debug两编译错及Flow安装前None unwrap是新tracer机械/oracle问题，分开保留。

2026-11-01 Main capture原RED完整保留：整链丢索引、foreign queue+cap共同替换、镜像丢失/错epoch及整mirror None均曾误退款；SourceCapture/Root冷资格共同闭合。后空Idle已结清仍旧epoch误Q的真实RED，用严格零债等待态关闭。编译签名/脚本部分失败与receipt/Rootmode测试oracle分别记录，不冒新生产缺陷；raw n1-main-capture-*，exact见统一结果顶部。

2026-10-31 Device cold：保n1-device-cold-*全部原件。真实RED包括invalid预算退Legacy、内部lane capacity漏Q、旧runtime仍启动、通知取消取走首错、source失败后仍等Node、observer未知仍Starting/deadlineNone。源码修于原cold/PumpControl/Service root；真正待握手用实际Initial到本地sink后注入65540帧锁定。两个oracle失败（误期待WouldBlock、9005未超65539）单独保留；缺API/旧take_failure入口编译错不冒运行缺陷。最终36源码/55raw/2pins独核有限签，无新增SIGABRT，前序全部保留。

2026-10-31 Native Link：真实last-Arc wake+Drop及tail自唤醒+实际D Drop两SIGABRT、IO5被retire抢位、同writer peak2、typed注入假首IO、未claim receipt假Settled、completion迟写全部保raw n1-native-link-*。另2个test shell缺字段E0063、旧3fixture未claim却期待0的8/3失败保留；只改真实结算调用，不弱化0/首错/期限/Q目标。本有限边界独审已通过；0误筛不计、扩大Service3缺旧key失败留C，exact在统一结果顶部。

2026-10-31：下一Device factory准备阶段真实prefix0 write成功但无TCP summary RED已保n1-device-factory-framing-red.log；cold.prefix解析后Device60/Loopback8/治理7+20有限独审。Native Loopback实际factory/背压尚未写，不能凭storage移动签交付。

2026-10-31：Device pump本轮真实SIGABRT、同步构造首错倒置、Transferred空slot误Q、PAUSE确认封口后Pending四RED及FnMut/缺lane API编译轮全部保n1-device-pump-*；最终59/7/真实session1、Proto545/Quinn93/治理7+20有限独审通过。RX原两RED仍保。完整factory/Loopback Egress/Main/global/A/B/C仍Open。


Device本轮（2026-10-31，n1-device-*）保四真实RED：Pending取消丢原6B packet、编码原IO被postQ换成BrokenPipe、首包中Q仍继续write、空summary的end_batch unknown未同步AppQ；均修于原Egress/Main结算owner并独审有限签。E0451/E0509/E0283三编译轮全保；两次误筛0项不计验收并补真实函数。原Service egress1pass/3在tuic.rs1485缺server-key.pem，未进新行为，留C且未造/覆盖旧secret。无新SIGABRT，历史全保。

Main本轮（2026-10-31，n1-main-storage-*）保原四编译轮、cold-green命名虽green实际TooLarge/Full分类RED；D16真实128KiB先取后64KiB拒使报告131072但pending/queue/cap0并Q，改原selection前限量；typed原gate只验len/permit使raw payload合法epoch进入，改原selected borrowed Node backing门。physical9/custody5/d6 2/merge2/治理7+20 GREEN；wire20/1仍原smoltcp tcp.rs186 overflow留C，非新回归掩盖。无新增SIGABRT，前序全部保持。

Service本轮保真失败（2026-10-31，raw n1-service-cold-*）：取消factory后父stop原期限延长3秒RED；bind+D Drop及notify+saved Drop两真实SIGABRT；phase锁内callback RED；确定性barrier证实先Q后retain令较晚Disposal抢first（4 vs3）。修原Control/saved owner、期限继承、锁外单runner、原Box/first先于Q；notification-leaf5/factory5/期限3/真Node1及治理7/20 GREEN、独审有限签。first-capture-red的.0残留E0609是编译轮，非功能RED；全部原raw和C失败保留。

Native输出（2026-10-31）全raw n1-native-output-*：真实Full消费树/裸Vec、covered零输出prefix不清、空旧slab晋升、原Q仍mint reader receipt、permit cap+1、末permit callback立即Q缺口均保原RED并闭。cold fault5用slice(0..0)实际释放backing，须truncate且先assert identity；hard夹具不能用observer晚挂账；ordered pump reader默认source_read_bytes=None，零读必须用原source/pump被动观测，不能补假Some0。误筛continuous0tests已保并补真实13/13。无新增SIGABRT，历史全部保留；缺旧private key三消费者仍C原失败，不生成/覆盖旧secret。有限独审只至RelayEvent与冷拒绝，Main/Device/global lane仍Open。

Ordered source/pump有限已签（2026-10-30）：cold-budget真实预检仍Ok、TooLarge误归unknown、锁内Waker/锁外Q后try重准入、hard票据误交unmetered recv在未spawn时假EOF均保原RED并关闭。3编译轮及两测试清理SIGABRT完整留存，测试断言展开造成的二次panic不冒自然wire生产SIGABRT；RawWaker断言须安全清理后再报且poison不等于仍持锁。fullQuinn89/4旧Pending空库存oracle改实际all-slot len＋Q已有值负例后93。过宽ordered_103/11的10missing私钥/1smoltcp溢出仍C原门，新增fixture仅内存生成私钥，未覆盖旧cert/dirty；详情统一结果n1-ordered-source-*。

Response有限已签（2026-10-30）：真cold scratch缺1470B RED；guard移除反事实重现默认无pacing send panic不即时Q；独审又发现局部planner漏完整Proto/Catalog/pacing导致exact-cold仍spawn但无法接收。前备完整Root后同预算one-copy核验、typed固定scratch和原RootPanic闭合；tight wire与最终538/93/root全绿。工具独审一次内容flag原任务不变重试成功，不是Rust失败或需要外部权限，未申请接口。原日志/反事实/有限审查范围见统一结果response顶部。

Pacing已有限签（2026-10-30）：保全部n1-endpoint-pacing-*。真实构造未扣布局、Proto Drained遗留记录、live facade去pacing、Full先改Path、explicit detach Drop不即时Q均已锁并修；retired A Drop→selected B裸Drop真实SIGABRT，选出即托管才闭。首Mutex.lock的1分配以分段计数定位，移cold初始化。E0608/E0308/E0609测试/接口编译轮全保。Path“双panic”原测试实际PASS，撤回该推断；只认different-now精确timer的明确scratch反事实RED。rsync -a恢复反事实源码mtime回退曾令full533/1仍用旧增量产物；仅touch scratch改动输入重编译后原栈/同源码Proto534全过。内容hash一致不证明编译产物已刷新。最终独审/计数由STATUS与统一结果拥有。

Fanout已有限签（2026-10-30）：原真实2Conn receiver wake→剩余ControlPermit Drop再wake SIGABRT，以及sole candidate query panic→last Arc Drop再panic SIGABRT均保raw。原反例未达到catch，不能倒推RED末Q。收敛同Root held lanes、先disarm后unlock、锁外wake/旧/候选socket disposal；candidate必须早于local_addr纳入custody，不只保护已替换旧socket。独审又以实际漏一票证明target→Catalog单向盘点会部分发布，补独立数量＋key/lane唯一双射及实际event/candidate身份，损坏Q。Native pending先take再验也曾clean吞Auth，必须取前拒绝。raw n1-endpoint-fanout-*全保，最终Quinn90+Doc1/原payload/即时Q/末1与独审有限通过；完整Endpoint余项Open。

PreparedReply本轮保真失败（2026-10-30，raw `n1-endpoint-reply-*`）：Root+receiver原3alloc；旧owner键先关successor、旧公开入口clean吞event、free链摘后审计可自环、server Legacy stamped key误作Native fixed active，HMAC在实际State→Root锁中panic后二次poison unwrap造成SIGABRT，修State后TLS导出仍可写输出。另借用E0503及测试FutureExt依赖错、错误bare末Drop Q oracle和无效server Rebind刺激分别保原日志。修复门是实际接收同Root、Root冻结profile、前审free链、State sticky Q及公开fatal门；有限review/完整511+78/增长7见统一结果，未签完整Endpoint。

2026-10-30 N1 previous pending/source: cold37→39 (test iterator tail borrow; runtime clock closure spanning a mutable State helper). Preserve exact call positions when shortening the borrow; do not add hot Arc cloning. hard≥29→30: real Rebind retained a legacy HashSet capacity3 in Native. early-clean≥29→30: actual A sender with valid B target was accepted, pending2→1/stats0→1/nonQ; target-only Root validation missed source authority. Real source RED now rejects/Q1 and keeps pending2/stats0; actual original senders recover normally. Final Quinn77+Doc1/root related GREEN, independent finite signoff; full fanout/Endpoint remain Open. Raw n1-endpoint-prevsocket-* and unique result own exact evidence.

2026-10-30 N1 Host/PreparedConnect: preserve every n1-endpoint-host-* raw epoch. cold-interface30→37: missing Duration, LocalCids API, Catalog delimiter/move, State fixture phase, fixture mut, public TransportParameters path. Same compile cause repeated is not a new round. hard≥28→≥29: real create_io_poller after publication under Endpoint Root lock; original actual drain/zero occurred before the RED assertion. New real preparation path observes(1,false,0,0).

Early-clean≥23→≥29: unpublished pacing record leftover; canceled generation borrowing a later settled watermark; closed Connection published; field Drop unwind issuing a disposal witness; poisoned pacing owner normalized from zero-looking fields; Host constructor Root poison causing cleanup SIGABRT. Raw runtime REDs and independent source findings remain distinct. Repair the shared actual disposal/publication/Root failure authority, not markers or copied refunds. Current regressions lock original Arc panic payload, no false ACK, true driver failure and sticky last Q1.

Q-shutdown1: opaque poller panic and published future dropped before first poll each correctly set Q, but the original Endpoint driver waited forever for normal terminal processing that Q rejected. Keep the unknown registration nonzero, stop the authority driver with Err and dispose its actual channels; do not allow ACK under Q or report normal driver completion. Normal Full/TLS refusal/known private cancellation stays separate and nonQ.

Final finite acceptance: Proto505+Doc3, Quinn69+Doc1 with original ignored, root related gates GREEN; see STATUS/result for exact epochs and remaining Open obligations. Test-only formatting is documented as such, never a fresh test run. Previous failures/counters are retained in archive/n1-host-prepared-20261030-ERRORS.md and stage results. The user's complete goal overrides repair-confirmation waiting; it does not open either guard or any external/commit/push/mail gate.

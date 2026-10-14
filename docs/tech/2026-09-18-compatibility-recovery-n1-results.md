# TUIC兼容恢复 N1 — 执行记录

2026-09-18。N0已接受；用户在多Agent执行说明后回复“继续”，本次按冻结N1卡启动。N1整体未完成，不授予N2–N5、外部验收或commit/push。

设计与唯一N1卡仍在[已接受N0结果](2026-09-18-compatibility-recovery-n0-results.md#n1-唯一执行卡--设计已接受rust-未授权)；该卡的“未授权”为N0接受时的历史停点，本次新增权限仅按authority.json。全路线清单仍为[指南§5](2026-09-14-compatibility-recovery-execution-guide.md#5-唯一剩余工作清单)。本文件只记录执行与证据，不再复制架构。

## M0 执行卡 — 已执行，结果见下

目标：原device8项/main2项inline tests外置，符号/断言/可达路径不变。非目标：新增stop/recovery行为、公共API变化、全仓格式化、P3改动。Writer只写src/device.rs、src/main.rs、新src/device/tests/mod.rs和src/main/tests/mod.rs；main必须显式#[path]。Integrator单独删除repository provenance的对应两项inline allowlist，不能增加任何例外；growth不需要扩大库存。

失败树：路径/父模块可见性错误→编译失败；遗漏/改变测试→exact数量或原文移动校验失败；allowlist未降低→provenance失败；其他Rust漂移→范围核对失败。已有N0同源码20次M0基线通过；本次先重跑M0 baseline，再纯Move，再同命令GREEN；一旦非预期失败停止，不以0项或删除测试过关。

实际命令/数量/hash保存于[本阶段证据](../current/n1-20260918-evidence/)。baseline default/all各device8＋main2；后续lib/bin两模式check、growth7/repository20/Knife220、strict治理与限定fmt/whitespace。Writer不得修改inventory；原Reviewer读实际变更和原文等价性，ReviewPending期间停写。任何repair-test失败按原规则先证据/最小方案再等用户确认。

开始状态：449Rust与N0检查点逐一匹配，HEAD64f027fc，脏区保留；source-before.json、m0-before.tar与Git stage记录可核对。Writer为/root/n1_writer，现已停写；root只做Integrator治理操作。此时无新Rust功能。

## M0 实际结果 — AcceptedLocal

Writer baseline/post各6命令20次通过。device812→632行，测试子模块183；main192→163行，测试子模块33，显式path正确。两份原测试body仅去掉一层缩进，规范化哈希与新body一致；生产前缀字节不变。Integrator仅从repository_test_module_provenance.rs删除两项已迁出的inline allowlist，421→419行；未修改code_growth库存或新增任何例外。

default/all各lib/bin check与growth7/repository20/Knife220通过，strict治理编译和7项、限定4文件fmt、git diff whitespace通过。共541次测试执行通过（baseline/post40＋两模式494＋strict7），不是541个唯一测试。post unit tests后只改独立provenance测试的allowlist，lib/bin及其测试依赖未变，故结果仍适用；源最终451文件在m0-source-final.json，stage entries相同。

运行器有两次工具层错误：误写不存在的knife16_implementation_provenance target，Cargo exit101且执行0tests；随后的metadata list-index assertion在任何测试/修改前失败。m0-integrator-05.log及m0-runner-classification.json保留证据；随后直接复用N0的knife16_owned_leg_provenance准确命令执行未运行门禁。不是Rust修复/业务RED，未进行源码repair retry；不能把错误调用记通过。

原wip_audit独立审查0/0/0 Approve，全部原文/451源码/hash/计数核对通过；只接受M0纯Move。M1随后按已授权卡开始。没有恢复/stop/join行为变化或外部验收。

## M1 执行卡 — AcceptedLocal，独立0/0/0

沿用N0同一卡，不扩范围：tuic.rs2411–2483及2537–3480共1017行完整native/Tracked readers移动到src/tuic/tcp_readers.rs；父模块两个既有testhelper保留，src/tuic/tests/mod.rs不改。只读预检确认combine_ordered_native_chunks与TrackedRelayStream.tcp_diag的原测试访问必须保留；6struct/2trait/8既有构造方法/1函数/1字段仅pub(super)，父模块cfg(test) imports维持旧测试super::*，无pub(crate)/公共raw能力/新wrapper。四处selfwake spawn及ordered-pump Drop行为原样留待N1-L。

验收：先default/all各12个N0卡精确测试GREEN；用已通过F1-R1的独立CA/CA:FALSEserverAuth临时身份且finally原cert恢复/key删除。再仅Move/import/私有可见性/模块说明，原test/helper/body逆变换核对；同24exact GREEN、两模式lib/bin、growth7/repository20/Knife220、strict及scopedfmt/whitespace。Writer不能改治理；报父文件精确行数后由Integrator降原7991库存，新文件≤1500，不扩too_many_arguments。出现任何真实门禁失败/范围外依赖则停写，依既有stop rule处理；不混入N1-L行为。

M1实际：baseline/post共48次exact通过；默认/all的lib/bin check、growth7/repository20/Knife220及strict7均通过。首次scopedfmt发现父模块一处双空行与两个import块排序差异，原exit1保留；同一Reviewer确认仅是首次Move格式收尾，无行为repair-test失败。Writer只做两处格式规范化，Integrator再降库存至6984；随后growth两模式、strict7、scopedfmt及此前未运行的diffcheck通过。共570次测试执行通过、0测试失败。不是恢复功能接受。最终452份源码在m1-source-final.json；临时证书恢复、私钥删除、Git stage条目不变。

原1017行slice的明确可见性/空白逆变换相同，child1048行；父两个helper与原tests字节不变。格式后的差异单独留在m1-format-delta.json，原plan与已测源码证据未覆盖；旧测试/编译证据对该纯格式差异仍适用。最后snapshot工具曾错误假定root benches/examples存在，六个门禁全部通过后才退出2；按原inventory的src/tests/third_party生成最终快照，未作Rust或测试修复。原wip_audit正式独立审查P0/P1/P2=0/0/0 Approve，仅接受纯Move，不接受N1-L/R、S1或P3。

## M2 执行卡 — AcceptedLocal，独立0/0/0

n0_contract确认原336行startup壳可完整移到compatibility_runtime child；只需一次public reexport保留start_tun_proxy API，run_event_loop_sinked仍私有。child访问父私有配置/helper不需pub(super)扩散；startup-only的VirtualTunDevice/FailoverUpstream/LoopProfiler/RealityUpstream/TuicClientConfig/TuicUpstream imports迁入child。原loop/body、测试文件及父文件末尾cfg(test) mod tests保持。两模式6exact分别为upstream_kind_selector与五个tun_runtime_config_*；没有真实TUN startup接受。

M2失败树与出口：原文及未变DATA loop不等→停止；import/reexport丢失→lib/bin编译失败；原6项缺失→exact非零计数失败；治理不降/来源漂移→门禁失败。先两模式12exact GREEN，再仅Move/import/必要doclink与格式，再同12exact GREEN；root随后按实际行数降client_tun库存，执行相同编译/治理/来源/strict/fmt门禁和原Reviewer审查。m2-before.tar保存原文件，Writer不改治理、不加新行为。真实repair-test失败照原规则停并请求具体修复确认；S1待答复不阻止本次已批准M2纯Move。

实际Move为父16561→16223/child360；原336行仅一处rustdoc链接规范化，逆变换SHA与原slice逐字节相同。原DATA loop、helper与旧tests不变；Integrator只降client_tun库存至16223。baseline/post共24次exact、两模式lib/bin编译、两模式growth7/repository20/Knife220、strict编译/7项、scopedfmt与diffcheck均通过，共525次测试执行通过、0失败，无新增工具错误。m2-source-final.json拥有最终453份Rust；978个Git stage entries不变。M2不接触证书/TUN/网络；原wip_audit最终P0/P1/P2=0/0/0 Approve，仅接受纯Move。

N1-M累计1636次通过（541+570+525，含重复执行），零实际测试失败；原工具/格式失败记录保留，最终受影响门禁通过。原449份Rust中443份不变、6份变化，新增4份、无删除；最终快照及三次review由n1-m-summary.json导航。N1整体未完成。N1-M接受时Writer与Integrator均停止Rust写，等待S1明确答复；随后用户批准记录见下。

## S1：N1-L 最小范围/API 修订 — 用户已批准，未实现

用户在本提案与N1-M结果后明确回复“允许”。s1-approval.json保存原文与原提案hash；冻结N1卡C已加入这项窄修订，原N0接受证据保留。现在可以按原N1授权实现相关N1-L/R，不再等待同一批准。

真实调用链为TuicUpstream::open_tcp_relay→TuicNativeOrderedPumpReader::spawn/spawn_from_source→tokio::spawn；唯一JoinHandle保存在reader.task，随后藏入Box<dyn NativeTcpReader>，NativeTcpRelayStream仅带reader/writer，LegacyOwnedUpstream转交后由spawn_remote_relay把reader移入supervisor。reader Drop只有abort，session无法取回并实际join；open取消/结果丢弃也可发生在pump已经启动之后。事后取handle不能关闭这些窗口。

冻结N0卡C没有src/upstream.rs，且明确“出现表外owner即停止该步骤，报告最小scope修订”。n0_contract与handoff_reader分别只读核实该缺口。候选只把src/upstream.rs::NativeTcpRelayStream及必要构造/拆分seam加入N1-L可写范围：它携带一个不可克隆、可选、尚未spawn的prepared pump控制载荷，不建立第二份registry；TUIC prepare，session预留flow与角色槽后，无await地spawn→登记唯一handle→发布。reader Drop仅请求stop，持久custody保留handle、实际join结果和有界首错；读阻塞与满channel均须响应stop。未spawn的open结果被丢弃时不得遗留运行任务。

NativeTcpReader、ProxyUpstream、OwnedUpstream的现有方法签名可保持；但public NativeTcpRelayStream的struct-literal构造方式会改变，明确是源码API修订。现有8处构造（TUIC1、harness5、client_tun测试1、legacy adapter测试1）在已批准文件内做必要适配，无pump者使用统一构造函数。控制载荷保持opaque，不公开任意spawn裸Future能力；pump的stream/generation lease保持到任务实际结束，不能随reader提前归还。固定可选pump仍计入原每flow五角色预算，不增加任务、timer或error槽；prepared future/停止信号及原64-chunk队列/producer持有数据必须在既有W/F custody中计量，不能据此声称字节预算自动成立或增加无界容器。

沿用同一N1卡的取消/发布失败/pressure/panic/stop/join与原路径测试；不新增架构sidecar、P3行为、依赖、外部验收或N2–N5权限。M1/M2纯Move不包含此变化；本次批准解除S1范围停点，不等于N1行为已实现或接受。

## N1-L L0 — bounded error，AcceptedLocal

沿用冻结卡B，不另建错误registry：errors.rs保留ClientError原语义，仅接private retained实现；新retained.rs≤1500行，retained/tests/mod.rs≤300行。Writer只写这3处，Integrator不同时写Rust。record不Clone/Copy，无String/Box/dyn Error/Any/JoinError成员，inline文本≤1024B、含padding总size≤1152B；保留来源、阶段、资源、attempt、kind/raw OS、可得code和UTF8事实，未知内容标opaque，文本损失显式标记。先只提供真实io/ClientError/JoinError消费转换，不造未来ledger或猜被既有ClientError抹去的协议码。

失败树按优先级：保留任意原对象→释放计数不归零/预算未知；格式化未知对象→poison Display/Debug测试panic；UTF8裁剪不正确→边界/原长/truncated断言失败；JoinError被正常化→实际panic/cancelled task的class断言失败。首tracer为shared::errors::retained::tests::captures_opaque_io_facts_without_formatting_or_retaining_payload。新type/API不存在导致的首次compile RED只算接口缺失，不冒称业务故障RED；之后必须实际执行1项GREEN。后续case逐个RED→GREEN，非预期repair-test失败停止并按项目规则请求具体修复确认。

真实消费下一步是device原owner首错槽与双阻塞stop/join，再进入session/relay custody；L0仅接受值转换，不能宣称总20975槽、ring、公开session或恢复路径成立。公开TunIo不能泄漏crate-private错误类型，L1采用封装公共结果/视图；harness真实LoopbackIngressPump必须显式stop/join，禁止默认立即clean绕过。原3个无任务mock可明确声明无后台任务。有限预检已定位这些既有scope，不再扩整体设计。

L0出口是两模式真实定向测试、lib/bin编译、原治理/来源门禁、独立审查；L1紧接生产pump tracer。当前Clean/System/DDIA局部设计评分为9/9/9：明确值边界、资源来源与固定容量；达到更高置信度仍需实际转换与device/session消费证据，不将设计分数当运行验收。

### L0 实际结果 — AcceptedLocal，独立0/0/0

只改errors.rs private seam，原ClientError字节不变；retained实现162行、新test index291行。两模式实测record1128B（编译assert≤1152）、文本1024B；非Clone/Copy，消费io/ClientError/真实JoinError/直接caught panic，无未知fmt或原对象留存。8case最终两模式16执行，加逐case16，共32通过；Integrator14门禁全部通过、501执行，L0合计533次通过、零实际test失败。6次缺失接口compile RED原日志保留，不冒称业务RED；首RED分类器E0412/E0425失配与两次未执行命令的字符串组装错误已单独分类，无Rust repair retry。

最终455份Rust为既有453中452不变、errors.rs接线变化、新增2份，无删除；978 stage entries/HEAD保留，无源码漂移。原wip_audit独立P0/P1/P2=0/0/0 Approve，Clean/System/DDIA9/9/9；l0-independent-review.json绑定source SHA010f46f8b50a5270b718c68d86c9c13e9c698da9a3f04349d0c23fcf37e54c27。仅接受值转换；device首槽/child guard/FailureId通知/总槽预算/session/恢复仍待实际接入。可得协议码必须在之后直接typed QUIC边界保留，不能猜已被字符串化抹去的code。

### L1 的有限接续（已授权N1范围，L1a开始）

两位只读Agent确认以下可编译次序；它是现N1卡的实现安排，不追加同义架构。L1a只写device.rs、原device/tests/mod.rs必要接线/签名适配和新lifecycle.rs叶≤800：一个原pump owner持stop、唯一handle、receiver/backlog、首错/终态，read/reserve/错误通知可停止；child poll/unwind边界立即转有界错误，结果/通知只带FailureId或固定终态，不等未消费JoinHandle积存opaque，也不加supervisor。观察取消保留handle，实际join后才取走；首错转移后仍为Failed。明确StopRequested/ConsumerClosed/UnexpectedEof/Failed，不以EOF/closed计数当clean。

首行为RED为真实spawn_tun_ingress_pump在ObservedReader确认底层Pending后关闭consumer，写端保持存活且不写额外packet，期望实际join。旧实现会超时；RED测试先abort＋await原handle再断言以回收测试资源。随后同一真实owner覆盖满队列stop、取消observer后重入、读错＋pressure、panic与重复观察；原8项保留。channel＋producer待发布包及实际backing均计数，stop的丢弃/剩余明确。只接受pump task/read-source/backlog结算，不冒称外层writer half、device rx/iface/tx队列或整个device已归还。

L1b在公开trait适配前，将client_tun/tests/mod.rs原13362–13814约453行完整设备helper群及相邻4项测试纯Move到已授权compatibility_lifecycle.rs；保留父调用DnsInjectRecorder/NoopTestUpstream及最小pub(super)可见性，断言不改、不复制helper。按旧/新限定名映射逐项GREEN→Move→GREEN，Integrator降低原14230库存。L1c再在同一闭合变更中让harness PacketLink提供整包source、替换重复pump并适配TunIo全部实现；生产AsyncRead adapter与harness保持平台/packet边界，静态Send future，不逐包装箱。harness原4104库存必须实质缩短，不能删空行换方法或默认clean。完整DeviceShutdown由L1c/session覆盖writer/backend及队列，再与session真实入口一起接受。

L1c现有harness范围补充：三项loopback收包槽测试需harness/all-features，default匹配0不得当通过。唯一旧with_ingress_pump消费者d16_real_quinn_forward_tun_batch_path_exceeds_capacity_without_ring_drops含32MiB传输与>170Mbit/s硬阈值；原Reviewer确认它是本地容量门禁，当前N1不运行/不改目标，不可冒充本轮回归通过。新增既定compatibility_lifecycle tracer须用替换后的实际Loopback pump验证stop/join/pressure；旧容量义务留N4本地性能矩阵/N5前置回归，不能把本地测试称N5外部验收。

L1a首行为RED已实际执行：device::tests::lifecycle::ingress_consumer_close_wakes_blocked_read_without_packet在default/all各1项失败、exit101，均命中预期消费者关闭后仍阻塞read的断言；没有编译/其他错误。ObservedReader证实底层Pending，写端始终存活且没有额外packet，50ms超时后先abort并实际await旧task，再报告RED。l1a-first-red-*保存两次失败与源码；它们是预先授权的TDD RED，不是实施后repair失败，不能记GREEN。旧8项两模式基线按相同device/index哈希复用M0证据，没有冒算为本次新执行。现进入同一有限owner实现。

L1a首GREEN已执行：同一真实消费者关闭tracer在default/all各1项通过，无实施后失败，见l1a-first-green-gates.json。当前继续pressure/stop/取消/panic/错误与实际backing结算矩阵；首项GREEN不构成L1a或整个device/session接受。

### L1a 实际结果 — AcceptedLocal，独立0/0/0

device985行、原test index201、新lifecycle叶569；只改device.rs/index，新增1叶，原TunIo及后续helpers字节不变。唯一handle用Running→Observed保存；read/reserve/consumer close可唤醒，Interrupted分支显式让出。child poll与inner销毁及时捕获panic，首错固定槽/有界记录/失败转移标记保持；producer/queue实际capacity按所有权转移，FIFO有效包在终态错误之前交付。DATA无新增全局锁、额外逐包分配或任务。Drop只stop，不能声明已观察join或整个device清零。

原8＋新11，最终两模式各19通过；逐case22＋最终38＝60次通过，Integrator14门禁501次通过，总561次。四项预授权行为RED（consumer-close、终态区分、Interrupted调度、FIFO error顺序）各两模式共8次预期失败，零非预期失败；旧8项必要签名/终态适配diff保留。l1a-writer-validation.json、l1a-integrator-gates.json和l1a-independent-review.json拥有完整证据。原wip_audit独立P0/P1/P2=0/0/0 Approve、局部9/9/9，绑定456源码SHA2243daf00353863339e44021ca2214bf9e044168a31d4b77146c3fce6bb17e99。较L0仅2原文件改＋1新，453不变，无删除；HEAD/978 stage不变。

只接受pump task/read-source/backlog局部生命周期，writer half、外层device队列、公开TunIo、harness替换、session和TUIC恢复未接受。下一步L1b严格按l1b-move-plan.json的453行原文与4exact新旧FQN进行GREEN→Move→GREEN；原文件/治理checkpoint已保存。Writer只写父test索引和既定compatibility_lifecycle叶，Integrator单独降低父14230库存，不增例外；本步骤不改任何生命周期行为。通过独立审查后才进入L1c共同pump/public接口。

### L1b 当前结果 — 未接受；真实来源门禁RED后停止

纯Move已完成：父14230→13779、新child458，root同步下降原test-parent库存，未改生产逻辑。baseline/post的4exact×两模式共16通过；撤销四处pub(super)并恢复末尾一个模块分隔LF后，原453行slice和整个父文件可逐字恢复。首次原文断言未预计rustfmt删除EOF分隔LF；原失败与外层未正确根据exit停止而继续只读post runner的编排错误均保留，l1b-format-classification.json只分类工具/格式事实，不能掩盖下面的真实门禁失败。

Integrator仅执行4/14门禁：default lib/bin check通过、growth7通过；repository_test_module_provenance实际19pass/1fail、exit101。失败为client_tun_test_module_stays_external_and_private（tests/repository_test_module_provenance.rs:349）仍只统计父和DNS child，得到334，期望338。实际搬移没有丢测试，但来源门禁未同步纳入新private child，是本阶段真实验收失败。总42pass/1fail；后10项未执行。原ReviewerRequestChanges，P0/P1/P2=0/1/0；绑定457源码SHA1cfa574386ae09915d3bc43a8f67fcc844a0bc8a3e0a15bff391b5a75e88a0e8。

首次失败停点（历史；后续R1已获批准并执行，见下）：依AGENTS repair-test stop rule，Rust Writer/Integrator当时停写。R1提案仅改原来源gate函数：复用已有scanner纳入新child4项，保持总338，锁定private/defaultpath/单一路径及原4test各一次、父无副本；不改生产代码、不降低义务。具体可审patch为l1b-r1-proposed.patch，尚未应用，等待用户本次确认。批准后先default/all source20，失败即再停；通过后完成受影响growth/strict与未跑门禁、原Reviewer复审，再进入L1c。HEAD/978stage与457源码无漂移，脏WIP与全部错误证据保留。L0/L1a接受仍有效，N1整体未完成。

当时原Reviewer只读判断R1具体patch可行；下面实际失败证明该提案审查漏核scanner支持的属性范围，不能继续把该判断当作修复证据。后续L1c只读预检已提出静态whole-packet source填充同一PumpPacket槽：空槽不分配，TUN adapter先按原MTU/PI尺寸adopt再await read，PacketLink pop后同步adopt原BytesMut，避免逐包MTU新分配/复制；Packet/Eof分开，generic pump不解释PI。该建议尚未写入/编译，不能视作L1c接受。

### L1b-R1 实际修复失败；R2 提案待确认

用户“开始”明确批准R1，l1b-r1-approval.json绑定所审patch。Integrator只改repository_test_module_provenance.rs的client_tun_test_module_stays_external_and_private，原函数之外字节不变；该文件450行，其他456份Rust无变化，before.tar和format.json保留。当前457源码manifest SHA707afa3c0756b4f9123ced3b9377513f61fb82227674faab0e4e41556b97f52d。

实际只执行default来源门禁：19pass/1fail、exit101，见l1b-r1-source-gates.json和l1b-r1-source-01.log。失败在368行：首项event_loop_stops_after_terminal_tun_failure识别为0，期望1。根因是复用的rust_source_scan::test_function_at只接受裸#[test]；虽然已支持async fn，却不识别四项共同的#[tokio::test]。这是本轮修复选择扫描器错误，原Reviewer提案审查也漏检，不是测试丢失或工具层错误。失败后立即停止，all-features来源及后续门禁未执行。L1b含此前42pass/1fail累计61pass/2个真实失败；不将已接受L1a预期TDD RED混算。P1保持，L1b未接受。

R2仅修改同一个来源gate：复用Cargo.toml已有syn2/full语法解析，逐项核实child中唯一直接函数、async签名和两段tokio::test属性，parent无同名函数，child无inline wrapper；保留private/unconditional/default-path单一路径、child4、全体338。共享scanner、四个测试body、生产代码和依赖均不变。具体可审补丁l1b-r2-proposed.patch，预计gate467行；仅作为提案保存，尚未应用/编译/测试。先两模式来源20，再四个迁出exact各两模式非零执行，再补受影响治理及未跑集成门禁，最后原Reviewer实际复审；任何修复失败再次停止。AGENTS repair-test stop rule要求新一次用户确认；R1“开始”不自动授予R2。

L1c只读预检另确认5个TunIo实现均需显式request_stop/poll_shutdown，无默认clean；公开结果封装私有有界错误且不可Clone，实际join后释放自身backend/writer及队列backing，错误转移后仍保持FailedSettled。Loopback只释放本端持有的PacketLink引用，不清空对端队列；Drop仅stop。停止后操作必须关闭，公开队列可被外部重新写入的事实要在重复shutdown观察中处理。此为既有范围的接入建议，尚未写代码或运行测试，不能替代L1b接受或session完整退出证明。

原wip_audit已只读核对R2具体补丁SHAaf1c851c852aded684c475e4b0fc6bd24a50b67b02bed082af7a7ca5b4578881，同意作为用户审批提案；不是修复或最终接受。457份Rust与R1失败时一致，Git stage/HEAD不变，见l1b-r1-stop-state.json。

### L1b-R2 用户已批准并执行 — 复验进行中

用户“确认”绑定原审核patch，l1b-r2-approval.json/before.tar保留授权与可恢复原gate。只改同一个函数，限定rustfmt后470行（提案467行的3行差异为格式展开），函数外字节不变；其他456份Rust不变，无新增/删除，Git stage不变。l1b-r2-source-final.json绑定当前冻结源码。按l1b-r2-validation-plan.json顺序执行来源两模式、四个迁出exact两模式，再受影响/未跑门禁；未得出接受结论，原两次失败保留。

### L1b 最终结果 — AcceptedLocal，独立0/0/0

R2的20条命令全部通过：来源default/all各20、四迁出exact两模式8次、growth两模式14、Knife两模式440、strict7，共509次测试执行；all lib/bin编译、限定fmt和diffcheck通过。default lib/bin沿用首次L1b在相同生产/Cargo源码上的成功日志，未冒称本次重跑。L1b累计570通过，原两次来源门禁失败保持历史事实。457源码无漂移，978stage/HEAD不变。原wip_audit核对真实代码、逆变换和全部日志后最终P0/P1/P2=0/0/0 Approve，绑定SHA9ac4e321ed0e2b62c26e69cefbf2aaf9c2e6f7a51b1195e664417f6c33307143，见l1b-r2-independent-review.json。只接受Move和门禁修正；L1c/session/恢复未接受。

### L1c 开工卡 — 共同packet source与设备stop/join

沿用上文有限接续与N0同一N1卡：Writer仅device.rs、device既定tests、harness原Loopback/PacketLink接线、compatibility_lifecycle叶及三个mock必要适配。新公开TunIo方法均必选，无默认clean。共同静态source填充原PumpPacket，PacketLink原BytesMut同步adopt，不复制/逐包MTU分配，空packet与AsyncRead EOF分开。一个pump owner保留唯一handle直到实际Ready；设备在实际join后释放自身writer/backend/队列backing，再报告Settled或粘性FailedSettled，观察取消不得丢owner。停止后不再产出设备token或注入新包；本端shutdown不销毁对端仍拥有的PacketLink队列。Drop只能stop。

首tracer为client_tun::tests::compatibility_lifecycle::loopback_stop_observes_join_after_whole_packet_handoff，harness feature才存在。真实Loopback容量1，先检查非空及len0/capacity>0包的原指针/内容/capacity，经真实wait_for_rx/rx_take交付；保持源活着且不送新包，在current_thread内stop后立即poll必须Pending，放弃该观察后再次poll_fn等实际join，最终及重复观察均Settled。当前无API，首RED明确只是缺失接口编译，不冒称旧pump运行失败；私有构造器接线错误另分类。随后依真实生产接口扩pressure、失败/错误移动、取消及退出入口关闭处置。新增测试一个一个推进，非预期repair-test失败按AGENTS停。

首命令使用--offline --locked --lib --features harness与--all-features各exact1，默认0不算通过。保留原device19两模式、四迁出exact两模式和既定三个harness收包槽测试；旧含32MiB/170Mbit阈值的D16容量测试仍未运行。Integrator在Writer交付后独占原growth精确降库存与来源inventory新增测试数更新；既有338项/四原测试名不可丢，不能等到门禁失败才想起同步计数。device≤1500、叶≤800、index≤300，harness4104及父13779仅能收缩；到界或范围外owner立即停，不能删空行/加豁免/新增旁路owner。

L1c首接口RED已执行：harness与all-features两次命令均exit101、零测试执行，符合预先冻结的缺DeviceShutdownState/request_stop/poll_shutdown接口预测，见l1c-first-interface-red-gates.json及同名source/log。不是业务失败，也不算通过；接下来实现同一完整设备边界后跑同一exact。

L1c首GREEN已实际完成：同一whole-packet/stop/join tracer在harness/all-features各exact1通过（共2次），见l1c-first-green-gates.json。原BytesMut指针/capacity与空packet语义、同turn Pending及取消观察后重入得到证据；尚不是完整设备矩阵或L1c接受。继续压力、错误move/sticky及关闭入口处置。

L1c首次源码复核补齐三个既定边界（不扩scope）：with_ingress_pump只准首次且未停止时装配，重复调用必须保留原handle；公开未sealed TunIo的实现可用DeviceShutdown::failed(ErrorKind)表达已结算失败，不公开opaque错误；N0卡B原additional_failures饱和计数在actual join后必须一次移入关闭结果，不能随pump/control销毁而丢失。计数为转交delta，重复观察为0而失败state/kind保持；后续session在唯一角色custody中饱和累计。本次不增加错误槽、任务或registry。Writer将这些与原压力/错误/重复观察矩阵一同锁定，预先声明的缺API编译RED与builder实际行为RED分别记录。

### L1c 最终结果 — AcceptedLocal，独立0/0/0

共同静态packet source复用L1a唯一pump/guard和backing归属；PacketLink原BytesMut同步adopt，空包保持packet，平台PI仍由TUN处理。TunIo显式stop/poll_shutdown没有默认clean；真实join后释放本端backend/writer和容器backing，观察取消不丢handle，重复观察保持失败。DeviceShutdown提供固定公开失败值，首错与额外错误delta分别单次转交；外层peer共享队列保留，重复builder不覆盖原任务也不在stop后启动新任务。没有新增task/timer/错误槽、全局DATA审计锁或逐包复制/MTU分配。

Writer只改device.rs1171、harness4099、device/tests/lifecycle749、compatibility_lifecycle707；index201/原device19及harness Generator标记之后原文保持，原4个compat测试只适配BrokenTunDevice::default构造。Integrator仅降harness4104→4099，并为新增3个harness限定测试把child4→7、树338→341，原4名/private/default/unconditional约束不减。最终457源码只6份变化，451不变，无新文件/删除；978stage/HEAD64f027fc保持。

7项新增case逐项两模式GREEN14，最终22条定向Cargo再通过66次（device原19+4、compat原4、旧memory3、新Loopback3按实际feature选择），Writer80。Integrator14命令全部通过：default/all lib/bin check、growth各7、repository各20、Knife各220、strict编译与7、限定fmt/diff；共501。总581次通过，非581个唯一测试。原6次缺接口compile RED为零测试执行；builder的2次预声明运行RED各0pass/1fail，旧实现遗失handle不能冒称已join。全部错误原日志保留，零非预期失败。原工具预编辑签名匹配assert在修改/Cargo前停止，首RED归档按其已有源码hash逐份恢复，详情与限制见l1c-writer-handoff.json；不是隐藏Rust重试。

原Reviewer最终P0/P1/P2=0/0/0 Approve，局部Clean/System/DDIA9/9/9，核对全部源hash/原日志与治理，绑定l1c-source-final.json SHA f18c054b8b8f67423ad0db9922ea84f8aab057a206af4b1f9da99dd0612bfa3c。l1c-validation-summary.json/l1c-independent-review.json拥有汇总/接受。既有warnings保留，不作warning-clean结论。

只接受设备局部控制边界。真实Loopback/pump已测；Virtual用例只构造已停止诊断壳验证重复队列释放与入口关闭，不是真TUN/backend/kernel-FD验收。不证明session/relay/节点TUIC恢复、整体20975错误槽、进程RSS归零、吞吐或P3闭环。原旧容量测试及外部验收仍未运行。

下一可执行门仍是同一N1卡的稳定session与open/flow custody。只读代码核对：当前两个loop按值持device并返回()，没有running参数；start_tun_proxy在稳定owner前创建TUN/pump再connect，DATA loop只在入口读错时break且尾部未await device shutdown。不能只Box现loop并外层catch，因为内层unwind会丢按值device。下一实现让device留在session保存的外层运行状态，原DATA future借用它；观察者取消重入同future，内层panic捕获后仍能实际join。真实loop tracer可从MetricsSink证明进入，并分别测试取消与enter_poll panic。SpawnedRemoteRelay当前不返回child handles，flow custody仍须按原N1卡完成；不以局部device结算替代。此为既定范围的实际接缝，不另建架构sidecar，不新增授权请求。

### L1d 接续开工 — 持久 session 的原 loop/device 边界

本次用户明确继续既有N1/S1，不重新申请；外部验收、commit/push、P3与N2–N5继续禁止。冷启动逐份核对457份Rust与L1c最终manifest一致、HEAD64f027fc/978stage entries不变，没有Cargo/rustc/peer；见l1d-cold-start-check.json。历史Reviewer /root/wip_audit本会话不可用，由新独立只读/root/n1_reviewer承担本次审查，不能冒称历史同一身份。单Rust Writer /root/n1_writer；root只写证据/文档及Writer冻结后的治理，另有只读custody scout。

先做L1d0纯Move：将原run_event_loop完整兼容组合wrapper及其文档移至已有compatibility_runtime，父保留同名公开reexport。此为既有启动组合的完整片段，不复制DATA loop，不改调用者或生命周期；原入口错误测试及M2六项characterization两模式GREEN→Move→GREEN、原文逆变换、编译/治理/来源和独立审查后，才进入行为修改。父16223须按实际收缩同步降ratchet，child360须≤1500。Writer仅两个生产文件；Integrator独占governance。

随后L1d继续同一N1卡：外层持久future保存device，原DATA future借用；run/observe_shutdown只观察同一保存future。分别锁定取消重入、内层poll/destructor panic后真实join、读错/公开shutdown kind-only首错、additional delta与未知shutdown panic非零保留。失败树：观察future被重建会使真实loop初始化/设备身份变化；内层按值device会在panic时丢失实际join；stop被当作完成会在current-thread同turn Pending断言失败；丢弃错误或重复delta会破坏粘性首错及一次转移断言。首个缺API RED与后续行为RED分别记录，真实repair-test失败停止，不作第二次随机修补。

test leaf只用已授权compatibility_relay_custody.rs，挂已有compatibility_runtime的私有cfg(test)/显式path seam，避免扩大旧test索引；上限800。该局部边界的接受只证明保存原loop和device custody；open/flow五角色/S1及节点TUIC closure仍为首完整路径必要项，未闭合前不切换生产启动。完整N1、恢复和所有原义务不减；唯一剩余路线仍为指南§5。

L1d0已独立AcceptedLocal0/0/0：原30行wrapper/doc byte-exact Move，原启动suffix及父逆变换一致；父16193/child391，Integrator同步降ratchet。baseline14＋post14＋治理501共529次通过，四个编译check在Writer实际执行并由Integrator明确复用，不重复计数；所有日志hash、457源码、HEAD/978stage均核对。新Reviewer/root/n1_reviewer绑定l1d0-source-final.json SHA2f156bf3673c9acca7b67f2586d4b651da6f62d10aea59cf1857b79ed4c1a1e1。只接受Move，随后按上述L1d实际行为测试开工。


### L1d 最终结果 — AcceptedLocal，持久原loop/device custody

新独立Reviewer /root/n1_reviewer最终Approve、P0/P1/P2=0/0/0，局部Module/Clean/System/DDIA9/9/9/9。绑定l1d-source-final.json SHA256 `5a0becfac3b31089336c8f84efc91b895de2ef3e4cbd55a6ac41d332abbc805b`，逐份核对458源码及全部实际日志；不冒用历史Reviewer身份。l1d-validation-summary.json、l1d-independent-review.json、l1d-writer-handoff.json拥有汇总、审查及Writer原证据。

实际Rust：compatibility_runtime内一个不可克隆ClientTunSession保存Pinned外层future；外层持device，原DATA loop改为借用并返回真实入口io错误。run/observe_shutdown仅观察同一future，取消不重建。内层poll与析构panic分开捕获后仍实际stop/join设备；未知pre-Ready stop/poll callback panic不重试不丢设备，sticky Incomplete保留同一future。first-stop的15s绝对deadline不随观察重置，到期仍可继续驱动。DeviceShutdown首错与饱和additional delta只转交一次；局部DeviceSettled/DeviceFailedSettled明确不代表所有session资源已归还。

独立源码审查发现并在各自首个预声明RED→GREEN中闭合两项P2：真实Ready后的诊断device Drop panic原会poison已保存future，现捕获并记精确DeviceDrop；state getter原在每次DATA poll取得control ledger锁，现Running先查stop atomic、不进入该锁。没有新增DATA控制锁、第二DATA loop、逐包库存扫描或新relay任务。公开by-value兼容wrapper保留旧返回/日志行为；startup/main/probe/harness仍走原入口，不激活半套session。

10项新tracer覆盖真实Loopback的观察取消、原loop poll panic及内层析构panic；有界控制fixture覆盖原入口OS事实、kind-only失败、未知callback quarantine、固定deadline与Ready后的诊断Drop；真实TunIngressPump源read失败加source Drop panic证明实际additional delta和饱和、重复观察不重计。Running锁用例由辅助线程实际持ledger、握手后立即poll原DATA，5s watchdog只防挂死并在解锁前标记失败；主线程先join辅助线程/结算设备再断言。没有用elapsed性能阈值冒充吞吐或成功TUIC对象。

Writer最终8条相关命令：default custody7/原compat4/device23/retained8；all custody10/原compat7/device23/retained8，共90次通过。Writer连同原冻结88及逐项GREEN累计193次；旧冻结源码/log不覆盖。Integrator14命令全部通过：两模式lib/bin check、growth各7、repository各20、Knife各220、strict编译与7、限定fmt、diffcheck，共501次。当前最终源码591次通过，L1d累计694次，均为执行数而非唯一测试数；L1d0纯Move的529次另计。2次缺factory/state接口compile RED均零执行；device Drop的两模式2次与Running锁1次为预声明行为RED，合计3次实际失败。零非预期repair-test失败；一次apply_patch上下文不匹配发生在改源/Cargo前，原工具事实留l1d-patch-context-note.json。

最终父16192、runtime668、retained167、新test叶741。Integrator只降父ratchet16193→16192，并扩既有来源门禁：原341/四旧名保留；syn AST锁一个private外置cfg(test) path及10个真实Tokio async test名，gate仍20项。对L1d0的457源码，5份既有文件变化、452不变、新增1份获准leaf、零删除；全部458再hash无漂移。HEAD64f027fc与978个stage entries字节保持，所有既有dirty/P3/失败证据保留，Writer与Cargo停止；见l1d-final-verification.json。

接受边界仅原loop/device持久控制。冷owner早于设备/异步初始化、生产入口cutover、W128/F4096五角色custody、S1 prepared pump、TUIC lifecycle/recovery和完整N1-L/R均仍未完成；无真TUN/backend/kernel/WAN/吞吐/全进程RSS接受。未commit/push/外部运行/发送完成邮件，未启动P3或N2–N5。

下一可执行门仍属原N1/S1授权：在同一session root准入并保有完整W，再登记F与五角色。process_listener_activity的准入须早于extract_socket_payload；channel128不能当worker上限。四类supervisor的独占child handles须从可取消栈归于持久custody；S1第五pump先prepare，预留角色后同步spawn→登记→发布，全部实际join前不归还F/lease。l1d-custody-readmap.json是只读符号图，非实现/验收；下一首测为实际原socket在W满时保留129th payload，含completed/unconsumed/late cleanup占槽。必须先预算获准测试空间：custody叶剩59、compat叶剩93、runtime剩832行；只读结构估算不等于完整scope可行证明。不能加未授权leaf/豁免、压缩空白、删矩阵或用第二authority绕过；若确认无法闭合，提出最小明确scope修订。指南§5继续是唯一剩余清单，不重开同义设计。

本次已到完整可审查的L1d边界；按AGENTS长会话规则整理接续，HANDOFF的“Ready-to-copy continuation at the accepted L1d boundary”可直接用于新会话。N1/S1授权持续，非repair-stop或重新申请许可；不得将本边界写成标准TUIC恢复完成。


### 2026-09-19 L1e 开工容量门 — S2 两条测试叶待批准，Rust 未改

继续原N1/S1授权，不重新申请其方向、接口或行为权限。按冷启动逐份核验458份Rust的hash/bytes/lines及独立路径枚举，全部与L1d最终manifest一致；HEAD64f027fc2fe3e123bcc8dafc7c482cd286896f4f、978个stage entries字节不变。当前代理树开工只有root；同目录上一任务idle、N0 notLoaded，无旧Writer/Cargo/rustc/sing-box。证据为`n1-20260918-evidence/l1e-cold-start-check.json`及`l1e-git-status-before.bin`。新只读Reviewer `/root/n1_capacity_review`和第二源码核对 `/root/n1_custody_path_audit`均未写Rust或运行Cargo，不冒用前次身份。

源码定位确认：`process_listener_activity:11402`在准入前提取原socket payload；`handle_local_payload:11666`的pending分支在消费后才检查256KiB buffer，失败会丢该payload；:11774的worker没有完整W custody，结果channel128不能当worker上限。`spawn_remote_relay:12698`及四类supervisor仍在可取消栈独占child handles；同一session root须同时覆盖结果未消费、late cleanup、顶任务失败和全部实际join。以上是待实现缺口的源码证据，不是本轮已执行的失败测试。

两位只读核对一致确认测试容量阻塞。runtime668/1500余832、父16192 shrink-only；获准session测试叶741/800与707/800合计只余152。实际LoopbackTunDevice/PacketLink可复用，但现有NoFlows拒open、NoopTestUpstream是成功壳，旧构包仅SYN inspector，均不具129条真实TCP的受控pending/result/late-cleanup fixture。首个完整W tracer按可读代码估计240–395行，完整open/flow矩阵还需四类engine/五角色。其他已准测试路径属于device/main/error/TUIC节点owner，不能借它们的空余绕过scope。数字为写前估算，未编译，不宣称数学最小或实现完成。

已形成具体[S2提案](../current/n1-20260918-evidence/l1e-test-scope-proposal.json)，只请求新增`src/client_tun/tests/compatibility_open_custody.rs`和`src/client_tun/tests/compatibility_flow_custody.rs`，各≤800行，挂现有custody叶的两个私有外置child seam；原341和custody10项身份保留，Integrator在既有syn来源门禁登记路径及真实测试身份。所有生产文件范围、单root、W128/F4096、五角色、任务/timer/error预算、1500/800/300与shrink-only、完整矩阵、repair-stop均不变。独立Reviewer已核对具体提案SHA256 `96af4e07e5f7ca615e9a6cfcd1c4f291d651ba868f035d107573317f4a00d171`并Approve、P0/P1/P2=0/0/0，见`l1e-scope-independent-review.json`；仅审提案质量，不代表用户授权或实现验收。不创建未批准Rust路径、不加豁免、不通过挪到其他owner、删测试或压缩空白凑容量。

生产root余832尚未证明不足，预计新增600–780行只是预算；实际若不能在≤1500下闭合则先停，不隐含批准额外生产文件。父四处约121行旧join控制是相关替换候选，D16原terminal/reader_done顺序必须保留；不能拿无关净零行抵扣复杂度。

下一执行出口已经具体：S2获准后指派一个Rust Writer，从`open_admission_retains_socket_bytes_until_custody_available`的真实session/原loop/内存TunIo路径开始；逐条RED→GREEN覆盖W waiting/worker/result/late cleanup、原socket字节及deadline，再接同root F/五角色/S1、实际join与错误保留。每个可编译边界走独立审查、两模式相关回归及原治理；预声明首RED不等于repair失败，真实repair-test失败必须保留现场并等具体修复确认。生产半套owner仍不切换，完整N1-L/R、TUIC恢复仍未完成。

本轮只更新文档与预检证据，Rust零改动、Cargo零调用、测试零执行、repair-test失败零；L1d历史591/694不是本轮复跑。S2在用户批准前保持ProposedNotAuthorized；N1/S1仍有效，P3/N2–N5、commit/push、外部/TUN/VPS/WAN/吞吐/M2和完成邮件均禁止。


### L1e S2 用户确认 — 恢复实施

用户明确回复“确认”，批准两条测试叶及私有路由/原来源门禁适配；s2-approval.json绑定原提案hash，不改写ProposedNotAuthorized历史。暂停后再次458源码、HEAD和978stage完全一致，无Cargo/rustc/peer；l1e-s2-resume-check.json拥有证据。恢复原N1/S1同root open/flow custody，先W128真实原socket准入tracer。原repair-test stop、单Writer、独立Reviewer、生产范围与所有禁止项保持。


L1e首个写包由唯一Rust Writer `/root/n1_writer`承担，Reviewer `/root/n1_capacity_review`独立只读，第二scout也只读；root仅文档/证据及Writer冻结后的治理。已批准S2不改原proposal/hash历史。首包接缝采用OpenDispatch围绕原完成channel，W保存在已保存outer future，inner只借用；private session不得绕过W，旧wrapper的过渡路由保留到后续完整cutover。

首测可证伪路径为真实第二smoltcp栈+Loopback设备：SUT RX4096，前128各发送首字节进入受控pending，129th发8192。在确认4096已ACK并完成额外SUT relay/iface/flush与generator循环后，4096仍未ACK；控制准入处只读原socket事实与wire证据交叉核对。一个error worker实际join且原结果消费后，129th才获准。后续逐条锁未消费结果、deadline/取消、保有成功结果与stop。SUT原configure_local_tcp_socket显式关闭ACK延迟，不能把库默认10ms当此路径的实际配置。

本包不把F/S1缺口隐去：成功结果未移交到F五角色前留在W，不能安装未登记relay；Native reader的abort/Drop不能算实际join。原完成通知可能先于taskreturn，必须非阻塞保有通知并继续poll原handle；不能在DATA loop无期限await，也不能提前释放W。精确TDD源与日志由Writer的`l1e-w-*`拥有；本段是执行计划，尚无测试通过声明。


### L1e-F1 实际夹具失败 — 停止，R1 具体修复待确认

唯一Writer新增`compatibility_open_custody.rs`199行、原custody叶741→745私有feature-gated外置路由；所有生产Rust不变。2026-09-19T06:03:34Z开始的唯一Cargo命令是all-features exact `client_tun::compatibility_runtime::tests::open_custody::open_admission_retains_socket_bytes_until_custody_available`，编译成功，测试0通过/1失败/0忽略，5.01s，exit101。原计划期望W无准入导致129worker对128断言失败；实际先在leaf181的5s setup超时，不能记为预期业务RED或任何实现接受。`l1e-w-admission-red-{plan,gates,source}.json`、原sources.tar和01.log保留，log SHA256 `49124de7ec49a66cd93c4c2009d8046b213c5600023a8f2b69cdeb255c33f558`。

日志从目标port10064开始明确`Capped`：fixture使用129个不同目标port，原`MAX_INTERCEPTED_PORTS=64`按设计拒新端口。root随后源码查出独立fixture缺口：PendingOpens未override`ProxyUpstream::open_is_cheap`默认true，LegacyOwnedUpstream透传导致原inline pending路由；此为源码预测，日志没报entered计数，不能声称实际已经证明entered=1。Writer立即停Rust/Cargo，没有自行修复重跑。459源码/hash冻结无漂移；原失败tar/日志不覆盖。

具体[L1e-F1-R1提案](../current/n1-20260918-evidence/l1e-r1-proposal.json)只改该新测试叶：单一目标192.0.2.10:10000、129个不同源port40000..40128，使用既有per-port pool129配置（小于global4096），显式false并断言真实async-open能力，按进入序号分配gate，等待129真实Established和前128pending后再发送129th payload。原64-port cap、W128、RX4096/last8192与真实两栈不改。保存setup及worker/window原事实后先请求并drive同一sessionstop、释放fixture gates、限时观察已进入OpenGuard归还；cleanup错误单记，不覆盖原失败，不把Guard归还当生产join。原red-plan中defaultACKdelay10ms错注保留历史，新proposal明确实际SUT为None，40turn是后续service-cycle证据。

独立Reviewer最初对缺失败前cleanup给提案0/0/1，保留`l1e-r1-review-v1.json`；补入新提案后，对SHA256 `2eadd97581abefd71c3891a6e5fd31bb4f4ace1429717c4c8a8e0735b13bd221`最终Approve0/0/0，见`l1e-r1-independent-review.json`。仅提案质量接受，不能替代用户下一次修复确认，也不接受本次测试或生产实现。

用户确认后才可修fixture并重跑同exact；必须先得到预声明W128断言RED才进入原N1生产GREEN。任何其他失败再次停止。W/F/五角色/S1代码均未开始，L1d仍为最后实现接受，N1整体仍未完成。所有原dirty/P3和失败证据保留；未commit/push/外部/TUN/VPS/WAN/N2–N5或完成邮件。


### L1e-F1-R1 用户确认 — 本次夹具修复获准

用户回复“确认”，批准hash `2eadd97581abefd71c3891a6e5fd31bb4f4ace1429717c4c8a8e0735b13bd221`的R1。恢复前459源码、HEAD及978stage字节不变，无Cargo/rustc/peer；见l1e-r1-resume-check.json与l1e-r1-approval.json。同一Writer只修新test叶并重跑原exact，必须先得到预声明W128行为RED，才继续原N1实现。原失败/提案审查P2保留，其他失败仍停。


### L1e-F1-R1 执行结果 — 有效 W128 行为 RED

用户已确认本次R1（l1e-r1-approval.json）。同一Writer仅修获批open测试叶，199→349行，SHA256 `896d7348401f6d37c1b6d64a77275b12ef6aebf847277387ee3c70f97b216f1d`；459份源码相对失败快照仅此叶变化，生产未改。独立Reviewer批准实际fixture0/0/0，见l1e-r1-fixture-independent-review.json。

获批同一all-features exact在2026-09-19 06:46:28–06:46:54 UTC编译并执行1项，0通过/1预声明失败/0意外失败：setup_ready=true、established129、initially_pending128、pressure_ready=true；W断言实际129/期望128，原wire window4096、queue0。cleanup单记entered129/dropped129/timed_out=false；旧DeviceSettled仅device边界，guardreturn不是生产workerjoin。日志 `l1e-r1-red-01.log` SHA256 `beb1630cb7103329b7a70548ce9a871b974c025269626c9e46b95a9078969f8f`；plan/source/tar/gates/classification全部以l1e-r1-red前缀保留，未覆盖前次fixture失败。

此结果排除了原夹具的端口容量/cheap分支问题，允许按既有N1/S1/S2继续W实施，不代表W/F或TUIC恢复接受。只读调度审计另定位原dirty stale-close与empty-close/reap的待决字节路径；证据在l1e-w-scheduling-audit.json，属于同open生命周期的后续确定性tracer。仍保持单RustWriter、Integrator独占治理及任何意外repair-test失败前停止。


### L1e-F2 — 首 W GREEN 编译失败，Rust/Cargo 冻结

R1有效业务RED之后，Writer按原N1/S1/S2实施W草稿：同一session持128槽与原JoinHandle、typed原完成通道、先准入后提取、20sdeadline、广播stop、有界错误及与设备共同shutdown。成功output仍留W，未实施F/S1/生产接入；不能按草稿描述冒称已经实现通过。父client_tun.rs16164（相对16192缩28）、runtime1191、open叶400、原tests/mod.rs13779、retained.rs171。当前exact候选绑定l1e-w-first-green-v2-source.json SHA256 `d6296581e8414daa70d6315306c28d289c8488b2d15b57478e5a6055fbabdfa8`；v1、v2快照及tar均保留。

获批原exact在2026-09-19 07:06:47–07:07:05 UTC运行并退出101：E0308，compatibility_lifecycle.rs399的drain_ready_tun_rx调用仍传mpsc::Sender，要求OpenDispatch。实际测试执行0项，没有GREEN或新的业务RED。日志l1e-w-first-green-01.log SHA256 `e94dfee351027772198da185bd552544c5e91bb3330c8dc00014a09d8a0100b3`；计划、gates、failure分类及signature-callsite审计完整保留。单Writer立即冻结且未自行修复/重跑，独立Reviewer确认同一错误及459份源码无漂移。

全仓只读审7个变更接口/27个调用，三个fixture constructor中两处tests/mod.rs已适配，仅lifecycle.rs377遗漏。R2提案只改该constructor为OpenDispatch::channel(1,None)，并把已发现的OpenDispatch::extract started/control额度门放在查空槽之前，使Full拒绝也消耗控制项。单次搜索最多128比较；32控制事务/Join项不能冒称32物理探测或公平性。R2未授权、未实施；再次任何意外编译/测试失败仍stop。

同轮容量审计：runtime1191余309，完整F/S1预计再需430–630；父约121行cleanup删除不能抵runtime额度，仅内部整理目前无可证的大量节省。可执行S3提案把原M2完整336行startup/profiler块纯Move到src/client_tun/compatibility_startup.rs；逆变换唯一文档链接后SHA仍为M2原 `cc808be746568fd808f9c44774255d6e1767dcf8cddfab8e6b9b7ec8e85d0407`。runtime用privatepath child和单entry reexport保父shrinkratchet，所有custody、testseam/FQN留原root。预计runtime约850，再加F约1280–1480；仅估算，上界余量薄，W冻结后须重算。S3新路径尚未授权/写入；须先R2与当前W边界GREEN，再单独纯Move绿到绿，再F行为，不提高1500限额或分散settlement。

现存W未完义务原样保留：256KiB/32MiB、无fixture-release stop与observer/actualjoin、deadline、completed/unconsumed/successretain、一次conversionDrop panic、FIN/reap/staleClosed、slot双向库存与控制公平性，以及既有回归/治理。F4096五角色、S1真实pump与lease、节点owner及生产cutover继续是完整N1硬门，不能改分为可选或N2。

Integrator只做文档/只读核对，未改治理或Rust。l1e-f2-freeze-verification.json确认HEAD、978stage字节相同，459源hash/bytes/lines无漂移、无cargo/rustc/peer；其中保留一次只读manifest字段名误读KeyError，未写源/运行测试。commit/push、外部/TUN/VPS/WAN/M2、P3、N2–N5及完成通知均未执行。

R2与S3最终独立提案审查各Approve0/0/0，分别绑定 `2567a90f20c153f35fe1270da84043da43cf1e500d0fd35d65cfe8ee4a9fc7e1`、`f83b80a0fd65a2b266a04cbfb9680d9eee141ce5be47ba05ef69379dfb2eacaa`；记录见l1e-r2-independent-review.json和l1e-s3-independent-review.json。仅提案质量接受，均待用户明确批准；没有新Rust或Cargo执行。


### L1e-F2-R2 与 S3 用户确认

用户在上述两项明确审批请求后回复“确认”，授权本次具体R2修复及条件S3新文件范围。分别见l1e-r2-approval.json与s3-approval.json，绑定原提案hash，不覆盖历史ProposedNotAuthorized记录。恢复前l1e-r2-resume-check.json核对459Rust与失败冻结清单无漂移、HEAD/978stage相同、无cargo/rustc/peer；同一Writer执行R2，Reviewer只读。S3仍须R2/当前W边界必要测试/独立review/growthGREEN及容量重算后才纯Move，不先拆文件，不扩大1500限额。后续任何意外repair-test失败继续stop，不重批原N1/S1/S2。


### R2 实际首 admission GREEN

同一Writer只改获批两文件：runtime1191→1193，lifecycle707不增；原open oracle400字节未动。459源绑定l1e-r2-source.json，l1e-r2-actual-independent-review.json批准此实际修复0/0/0。原exact在07:58:40–07:59:16 UTC编译并执行1项、1通过、0失败，测试0.60s；log l1e-r2-green-01.log SHA256 `9edf2b2456d550588bdb8754940e46bd0bb1f5ebf3b632313cd50dd131524172`，gates/源码/tar全保留。独立Reviewer核对非零执行及同hash，见l1e-r2-green-independent-review.json。

此项实际证明W128满时同原socket/epoch的4096B和fingerprint保持、wire window0/对端4096B backlog；一条失败worker实际join并经原handler处置后，同阻塞socket下一epoch获准。fixture收尾releaseall仍不证明广播stop。下一必需项为不释放gate的stop/observer/原handles归还，以及W剩余bytes/deadline/result/error/FIN/stale/inventory矩阵和回归治理。S3未启动，完整N1未接受。


### W broadcast stop 局部 GREEN — 不释放 fixture gate

只改open test叶400→523，初始化helper保持原admission语义、断言与FQN；生产仍R2。新exact `open_stop_joins_all_workers_after_observer_cancellation_without_gate_release` 实际1通过/0失败（0.50s），原admission回归1通过/0失败（0.58s）。l1e-w-stop-{source,plan,gates}.json与sources.tar保留；两日志SHA分别 `5ca6fc347b321e22bd2645859f480f15737b2b0b1c4fcc499f0222e260c35e8e`、`6ce0d7ea8c7556a5add711576ac1a653050e7c6a10e5529cc564f0c49cfd716e`。独立Reviewer源码及实际日志0/0/0，见l1e-w-stop-independent-review.json。

任何gate释放前，测试建立真实128pending，第一次shutdown observer明确Pending后取消；同session重入维持原绝对deadline，原slot/Worker/live为128，最终全部为0，announced为0，128个Guard退出。原观察先capture再fallback cleanup及断言，故此项现在证明生产广播停止与实际worker join；不把fixture救援收尾当成功。尚未接受其余W矩阵、F/S1 latepump、冷启动接入或TUIC恢复。下一测试完成未消费结果和error Drop后冻结核算完整测试容量，S3仍未启动。


### W completed-unconsumed / error Drop 局部 GREEN — 三项冻结，核算测试容量

open叶523→643，生产与R2不变。新exact `open_error_drop_panic_retains_completed_worker_until_notice_join_and_disposition` 实际1通过/0失败（0.49s），log SHA256 `44b86532caa140a7111d0a721bb105e763fec81fcb3581138840bcf35e30f4d3`。同冻结源的open_custody prefix随后实际3通过/0失败，log SHA256 `adecbcbdae9a5fd0ff91e5eb5f2ac4f94452370beac06afd37ab0207b58b25f4`；对应l1e-w-error-drop与l1e-w-three的plan/gates和源码tar保留。Reviewer独立源码/日志复核0/0/0，见l1e-w-three-independent-review.json；本次R2起累计7次通过，只有3个唯一新测试，不复用L1d591次作本轮证据。

新tracer实际先让一个原worker完成但不poll DATA消费通知，slots/Worker/live仍128；恢复原loop实际join并经原handler处置后均127。未知io Error的析构只panic一次，首错保留为opaque TcpOpen Panic、无原内容、额外错0；先捕获原观察再保底cleanup。此范围通过不表示完整W、F/S1或生产TUIC接受。

Writer随后冻结Rust/Cargo；l1e-w-three-freeze-verification.json逐份459源码无漂移，HEAD64f027fc/978stage字节一致、无Cargo/rustc/peer。open叶余157；只读文本分析把重复capture/setup形成helper仅省17行至626，余174，尚未写入/编译/测试候选。分析中一次stdin候选把helper排在模块doc之前导致rustfmt E0753；实际Rust未写且Cargo未运行，完整工具事实保留l1e-w-capacity-inspection.json，不冒称候选通过或真实源码repair失败。当前只读完成剩余矩阵容量与接缝复核；S3/治理未启动，不能先超限或借flow叶掩盖W容量。

独立库存审查未发现正常运行能生成orphan announcement，但现stop仅sum Some/live，非法slots0/live0/announced非零可能早退clean；这仍是既定异常fail-closed契约的待测缺口，不是已复现的业务故障。字节预算暂按N0的128×pending256KiB=32MiB持久等待队列；既有DATA串行同步转交scratch另有≤256KiB，没有额外await/queue。后续tracer须独立计pending sum与原socket保留，不把此数写成全部物理payload或RSS上限。


### S4 完整 W/F 测试容量提案 — 独立通过，待用户确认

已编译GREEN的open叶643行，真实capture/setup去重的文本估算仅省17至626；剩余174行不能容纳完整剩W矩阵。Writer逐case给出545–700行（byte/独立sum125–160、20s/取消/直接stop55–70、通知/32项/1Connect/库存90–115、poll/Drop/成功保留90–115、真实FIN/reap/stale90–115、原UDP/DNS65–85、共用glue30–40）；原父增加配置/观察预计690–735。均为未实现估算，不是可编译容量保证。

最小深切片为新增private child `src/client_tun/tests/compatibility_open_settlement.rs`，使用原open父同一真实129TCP fixture，原3FQN与断言留原位；不增加production可见性、第二owner或新index。F独立估算660–980也公开：主flow叶的共享真实八engine fixture与准入/登记/压力base330–510、含余量建议630；条件新增 `src/client_tun/tests/compatibility_flow_settlement.rs`只在fresh实际预算证明原800无法完整闭合时使用，同fixture角色失败/取消/latepump/实际joinbase330–470、含余量建议590。两个新叶仍各≤800；不借flow叶塞纯W，不删矩阵，不压排版凑限额。

S3既有条件明确为pre-F W局部门：pending/error/retained成功、实际W join、cap/deadline/FIN/stale/库存、32控制项/1Connect和原DNS/UDP/其他pending TCP，须测试/review/growthGREEN。已安装健康TCP公平、真实W→F成功/压力移交与Native latepump/lease实际join依赖F/S1，仍是完整N1硬门，不能用pending TCP或abort/Drop替代；本澄清不提前接受完整W。当前1193经S3和剩W预计872–912，随后F430–630导致1302–1542，上界仍可能超过1500；本S4不给production新路径、超限或拆owner豁免。

完整提案 `l1e-s4-test-scope-proposal.json` SHA256 `ed7c69f156716b0ff51747e65df2c32ccd0f07d1180888ef7a403259c3353d14` 已获独立Reviewer /root/n1_capacity_review proposal-quality Approve、P0/P1/P2=0/0/0；见l1e-s4-independent-review.json。仅提案质量接受，S4尚未用户授权、未创建新文件，S3未启动。N0卡C冻结精确test路径并要求无法闭合必须停报，因此本次只请求这两个明确路径的修订，不重批N1/S1/S2/S3。

最终文档同步的一次Python多行字面量SyntaxError发生在任何执行/写文件之前，已更正引号，仅文档脚本；无Rust/Cargo/test调用，见l1e-s4-doc-update-tooling.json。原源码freeze、所有失败、用户dirty与禁止项保持。


### S4 用户确认 — 恢复 pre-F W 实施

用户明确回复“确认”，批准最终提案hash `ed7c69f156716b0ff51747e65df2c32ccd0f07d1180888ef7a403259c3353d14` 的两个精确私有测试路径，其中F settlement仍条件性使用；授权见s4-approval.json。l1e-s4-resume-check.json复核459源码/hash/bytes/lines和路径集合无漂移、HEAD64f027fc与978stage字节保持、无Cargo/rustc/peer；同一Writer/Reviewer/scout在恢复前全部completed。恢复原Writer实施W测试child和剩余真实控制矩阵，Reviewer只读，root治理在Writer冻结后才执行。S3仍待pre-F W必要测试/review/growthGREEN及重算容量，不与行为包混做。原repair-stop与所有scope/non-goals保持，不重复请求N1/S1/S2/S3。


### L1e-F3 — deadline首测暴露W分批join唤醒缺口，停止待R3

S4批准后只改测试：open父643→646（privatechild路由，原3body/FQN不动），新settlement179；production仍runtime1193/父16164。首v1源/计划/tar未执行，预检依据锁定Tokio1.52.0的time/source.rs17–19毫秒上取整，将观察点明确为first−1ms前和first+1ms后，有限yield要求0<finished<128，恢复真实clock后才stop；这是首次运行前修订。v2实际源码独立预检0/0/0后执行首exact。

2026-09-19 09:55:54–09:56:24 UTC，all-features exact open_deadline_survives_observer_cancel_and_unconsumed_timeout_joins_on_stop编译成功，实际0通过/1失败，测试5.46s。原计划directGREEN，因此这是非预期失败，不是预声明RED。log l1e-w-deadline-v2-01.log SHA256 a105999afa2696c2e19daa11baebb0a3cf221ff8de6d2d1c0a26d67098346e98，gates/source/tar及l1e-w-deadline-failure.json完整保留，后续prefix4未执行。

实际setup128、20s范围、observer Pending与原deadline不变均为真；首deadline后1个worker finished，未消费结果故slots/Worker/live仍128。直接same-session stop在5s后stateNone，slots/Worker/live各2、announced0，128个OpenGuard退出；首错是bounded TcpOpen TimedOut，additional0。未采集剩2的is_finished，不能据guard退出冒称实际join。fallback额外poll后DeviceFailedSettled且未超时，不改变原失败。Writer立即冻结Rust/Cargo，未修、重跑、启动orphan/S3或治理。

l1e-f3-freeze-verification.json核460源码/hash/bytes/lines及路径无漂移，v2 manifest SHA256 c683210bc3d191a3e030aab8d534c93e84abcc04fa9a7ab5631de9c779e3e841；HEAD64f027fc/978stage字节不变、无cargo/rustc/peer。独立Reviewer复核同日志/源。源码可证伪解释在SessionControl::settle_opens：早32槽chunk的JoinHandle completion或Tokio coop wake可能被后续budget selfwake合并消费；尾chunk仅因历史pending而park，未保存下一轮重试需求。精确schedule尚待确定性tracer，不把静态推导写成已完整复现。

R3只修原owner的停止唤醒记录：一个持久于saved settle future的Arc signal，AtomicBool retry加Mutex<Option<Waker>>；每poll登记当前observer，Waker clone/drop/wake callback均在锁外。child/coop wake先置flag再take注册并唤醒，budget selfwake直cx不置flag。新全轮开始清旧flag，四批每次仍≤32实际join polls，尾轮仅有retry时续扫，否则park。禁止无条件selfwake忙扫、一次128全扫、以guard/is_finished/abort作结算或放宽deadline/assert；不新增dependency/path/task/timer/queue/authority/DATA改动。

具体l1e-r3-proposal.json SHA256 e7db0a3e4e2dd380a04235abb97cac64cd6e4fad17b79a3b78ac54e521df6130已获Reviewer proposal-quality Approve、P0/P1/P2=0/0/0，见l1e-r3-independent-review.json；用户确认前不得下一次修复。批准后的序列：既有S4 child新增真实JoinHandles槽0/64+计数Waker确定性交错RED，同pinned settle future在96chunk换observer，锁重试不丢/真实Pending不忙扫/当前observer收到wake/实际join；然后局部production GREEN、原失败deadline体原样1pass、open prefix5pass。其它任何失败再停。runtime增40–70/test增90–140仅估算，需实际复核限额和后续S3/F容量，不授予超限。

N1/S1/S2/S4及条件S3均保持。本次确认后实际新通过数0，上一轮7次通过不能复用为本轮结果。完整pre-F W、F/S1、节点/生产入口与TUIC恢复均未接受。文档同步曾有一次functions.exec外层JavaScript parse error，发生在任何nested tool/写文件之前，无Rust/Cargo/test影响；拆成较小文档命令后完成，见l1e-r3-doc-update-tooling.json。全部dirty/失败和禁止项保持。


### L1e-F3-R3 用户确认 — 恢复确定性RED与局部修复

用户明确回复“确认”，批准提案hash e7db0a3e4e2dd380a04235abb97cac64cd6e4fad17b79a3b78ac54e521df6130的具体序列，见l1e-r3-approval.json；原ProposedNotAuthorized历史不覆盖。l1e-r3-resume-check.json核460源与failed v2一致、HEAD64f027fc/978stage保持、无cargo/rustc/peer；原Writer/Reviewer/scout均completed后恢复同一Writer及只读Reviewer。先在原production上得到声明的真实JoinHandle交错RED，再局部wake修复GREEN、原deadline unchanged exact1、prefix5；任何其它失败再次stop。此为授权记录，尚无新修复通过声明。


### R3 确定性 RED 已确认

R3获准后仅在既有S4 child增加真实JoinHandles/计数Waker tracer；child179→330，原deadline5797字节前缀与production完全不变。v1在运行前自审发现一表达式两次opens取锁的临时guard重入风险，改成明确单guard后冻结v2；v1未执行，源/tar/plan均保留。新增151行比原90–140估计多11，实际仍330/800，后续容量须滚动核算。

2026-09-19 10:28:27–10:29:00 UTC首exact实际0通过/1预声明失败：唯一panic是completion wake must survive the remaining stop chunks。gates/log SHA256 db1c22299a8cd4460b78d4dc4d543266dcb3dfbb88f09f9e7ecf5df0e7d04391，见l1e-r3-red-v2-*与classification。原facts依次为pending=true、first_finished=true、retry=false、actual_first_join=true、quiet=true、new_observer_notified=false、old_unchanged=false、settled=true；最终slots/Worker/live/announced全0、guards2，cleanup完成且库存一致。不是夹具或编译失败。Reviewer独立核实此有效业务RED，见l1e-r3-red-independent-review.json。按已有R3授权进入精确同owner修复，不重新申请；原deadline与prefix5仍待真实GREEN。


### R3 实际 GREEN — 局部接受，继续原 N1

同一Writer只改runtime的stop-local唤醒记录，1193→1246（+53）；既有测试和原deadline完整5797字节不变。v1未跑，Reviewer在运行前要求wrap只load retry、仅cursor0新全轮清除，以严格对齐已批race proof；v1与v2快照均保留。没有新依赖、路径、任务、timer、queue或DATA接线。

2026-09-19 10:33:55–10:34:41 UTC，l1e-r3-green-v2-gates.json记录wake exact1/0、原deadline exact1/0、prefix5/0，共7次执行通过、5个唯一测试、零失败。三log SHA依次6e4846e21775f7f9c6f54f6e843e1d0961808e863736aad10e7f90caf1a70d70、19c96508f78db1eede4940ae9973a59b4ee815b05b5847ff81b31acd65d108c6、21224d921eb885ec1abe6eae2cc536ca09178184dc8c97c34029e26b9457b75a。独立Reviewer及Integrator各自核实460源hash/bytes/lines/path无漂移，HEAD64f027fc及978stage字节保持；见l1e-r3-green-independent-review.json与l1e-r3-green-root-verification.json。Reviewer给出P0/P1/P2=0/0/0，仅接受此局部修复；F3原0/1及fallback事实永久保留。

实际容量runtime1246、parent16164（治理当前16192）、open646、settlement330。R3新增151测试行不能替代原normal notice-before-return义务；后续W/F预算须计入，不能照抄S4旧估计。继续指南§5完整pre-F W余项与独立/治理门，S4范围内可做真实fixture共用的单独green-to-green整理。S3未开始，F/S1/W→F/节点owner/生产入口/TUIC恢复均未接受；所有禁止项保持，无须重批已有N1授权。


R3后只读容量复核（l1e-post-r3-capacity-audit.json）按当前实际源重算：S3后runtime约900–910，余pre-F W30–70与F430–630，合计1360–1610，且尚不含节点/cold-start接入；W两test叶经共享helper后的完整估计1433–1608。仅说明有可执行容纳机会，不保证1500/各800已闭合。后续按实际改动重算；不以父cleanup删除抵child额度，不删义务。orphan注入仅证明fail-closed，不能宣称原loop自然产生该状态。


### W共享fixture整理 — 单独green-to-green，局部接受

既有S4两叶内把811字节capture/ready与Duration import原样移父，原3测试复用相同setup/poll-catch并在cleanup前drop原scenario；原5个FQN及完整oracle/cleanup顺序保留，child自poll_once起全字节不变。parent646→630、child330→303，合计净减43，production完全未改。l1e-w-helper-move-*拥有准确源/plan/tar；2026-09-19 10:45:01–10:45:24 UTC同prefix实际5通过/0失败，log SHA4c992bab93273728c18a990f3177fcf2682a10606260b166bc7454cd2bf5ccc3。Reviewer独立复核0/0/0见同名independent-review.json，仅接受行为保持整理。下一包为原stop入口的orphan标记故障注入，不把异常状态称为自然运行反例。


### W orphan库存故障注入 — 预声明RED确认，进入同owner GREEN

原session初始run Pending且W为空后仅注入bit127，真实两次shutdown均返回DeviceSettled；原bit持续1、slots/Worker/live/guards全部0、绝对deadline不变、cleanup无超时。l1e-w-orphan-red-*记录唯一预声明state断言0/1，log SHA16fce8a5c75877b50d7dd281e66b6cd907471140f74a1c87ae5ab2a99a92028b；Reviewer独立确认不是fixture失败，原N1授权可继续GREEN。此为防御性fault injection，不能称为正常路径可自然产生orphan。

最小同owner收口为settle_opens入口现有库存scan生成Worker mask并验证announced⇒Worker，非法bit触发sticky quarantine且保留；合法Worker无bit不算错误。后续clear只在Worker实际poll Ready后执行，不能无条件擦去非Worker非法bit；其他合法Worker仍须结算。该单空W tracer不替代非Worker-bit/slot-live失配及其他完整矩阵。测试child303→367，production在RED时仍1246。


### W orphan GREEN — 局部接受

原owner生产修改共16行，runtime1246→1262；超出原8–14估计但仍≤1500，未压缩代码抵消。2026-09-19 10:51:12–10:51:34 UTC exact1及prefix6实际均通过（7次执行、6唯一测试），log SHA分别c5364836c79c6e94f80e3073ec1bb64dadee06283a417d258dd3199d8756a725和2bb655328c1954c49376d7a10439a44f943e5f66a4b51f651caa997b92814172；源/plan/gates见l1e-w-orphan-green-*。Reviewer独立核实460源无漂移、断言未改，P0/P1/P2=0/0/0仅接受空W orphan fail-closed边界，见l1e-w-orphan-independent-review.json。

最新实际open630/settlement367（合997），剩余两叶容量603行，完整余项仍需滚动重算。bytes只读预检发现默认RX4096在cap−1处自然环绕，+2可能被1+1掩盖，必须用4097或证明等效几何并观察原recv连续片段2、limit1/consume1/RX2→1。完整字节case含fixture/原ctx独立sum及deadline oracle估170–255，不能只报case body。下一正常notice-before-return characterization沿用原singlechannel/真实JoinHandle；所有其余N1义务及禁止项保持。


### W正常通知/完成竞态 — 组件局部接受

仅既有settlement child增加151行至518（原估90–115），原六测试body和production保持；组件实际使用原singlechannel与真实Tokio JoinHandle，锁定通知早于task return、取消旧recv observer并注册新observer、完成未consume仍W1、实际join后AwaitingDisposition仍W1、原consumed后归W并保留有界首错。不能把此组件证据等同原loop公平性或完整W。2026-09-19 10:57:44–10:58:06 UTC exact1/prefix7全通过，8次执行、7个唯一open测试；logs SHA9aa444d77829ebdddf81c9b1d546d7cff94174278d37a1089a0b8b20e80b2951、af98af5273eb58bba294382b5a03bfd903ccc581c536a4fd53feed02f7a0001b。l1e-w-notice-*与independent-review/freeze-verification拥有证据，Reviewer独立0/0/0。

当前460源无漂移，HEAD64f027fc/978stage字节保持；runtime1262、父16164、open630、settlement518。两条既有all-feature warning在L1d原日志已存在，未自动清理。Writer在可编译GREEN边界冻结，**不是repair-test失败**；完整pre-F W与治理尚未接受。

### S5 精确测试范围提案 — 尚未授权

当前两测试叶余452行，完整余项经Writer与只读scout逐真实接口核算为470–715行；helper整理已实际节省43，normal-notice没有新增可供bytes/FIN共用的observer。expected-inventory常量构造器还可省数十行，但不能证明完整矩阵及修正余量；不声称数学上不可能。完整容量事实见l1e-w-remaining-capacity-after-notice.json。

仅请求新增私有`src/client_tun/tests/compatibility_open_pressure.rs`（≤800），挂原open父、共用唯一真实fixture；原7个FQN/body不移动，不借flow叶装W，不改生产scope或任何限额。它负责真实bytes/追加deadline、FIN/reap/stale和DNS/UDP/32项-1Connect/cheaptrue及压力专用只读observer/wire oracle。原父只扩共享fixture（最终估740–790），原settlement保留成功/非法库存/pollDrop余项（估628–698），新pressure估300–550；逐包实际计数，不能把估计当超限许可。

精确提案l1e-s5-test-scope-proposal.json SHA f5e48fd5eba108d0a62db01a3dc5ac18cabede161d5700636f2ae2ef59e60dfb，当前仅交独立审查，未用户批准且新路径不存在。请求依据是N0卡C节的“新private test路径限于 ... 若实际无法在此scope闭合必须停并报告”，S2/S4只扩各自列名路径；已有N1/S1/S2/S3/S4不重新申请。

生产容量另行诚实保留：S3后runtime估916–926，余W30–70、F430–630，合计1376–1626且未含节点/cold-start接入。S5不解决或豁免该风险；先完整pre-F W与治理、条件S3纯Move，首次F行为写入前按实际源和完整事务作容量决策。所有原失败、dirty、完整N1义务和禁止项保持。


S5提案已获同一Reviewer独立Approve、P0/P1/P2=0/0/0，精确SHA仍f5e48fd5eba108d0a62db01a3dc5ac18cabede161d5700636f2ae2ef59e60dfb，见l1e-s5-independent-review.json。此仅提案质量结论，不是用户授权或实现接受；Writer/Reviewer已完成并保持Rust/Cargo冻结，新pressure路径不存在。恢复文本已更新到HANDOFF的S5 test-scope gate，N1/S1及所有已有授权保持。


### S5 用户确认 — 已恢复完整pre-F W实施

用户明确回复“确认”，批准仅新增pressure私有测试叶的原S5提案，SHA f5e48fd5eba108d0a62db01a3dc5ac18cabede161d5700636f2ae2ef59e60dfb，实际授权见s5-approval.json。恢复前460源逐hash/bytes/lines/path与notice冻结完全一致，HEAD64f027fc及978stage字节不变，无Cargo/rustc/peer、原Writer/Reviewer/scout均completed，见l1e-s5-resume-check.json。恢复同一唯一Rust Writer和独立只读Reviewer；根只记文档/证据，完整Writer冻结前不做Rust治理。首包为复用fixture配置与观察接缝的原7项green-to-green，再真实bytecap/独立32MiB/追加deadline等余项；当前此恢复记录尚不增加测试通过数。S3、F/S1及节点/cold-start/恢复义务与所有限额/禁止项保持，无需重新申请已有授权。


### S5首包：默认fixture与只读观察接缝 — 局部green-to-green接受

原7个open测试body逐字保留，默认RX4096、发送目标[1×128,8192]与send_batch=None语义保持。唯一真实fixture增加可选rx4097/分批配置；新pressure叶拥有只读PressureProbe与计算，窄type bridge/字段/原extract和dirty终点调用全部仅cfg(test+harness)，产品编译不新增锁或callback。fresh原SocketCtx求和只在显式请求后执行；不修改准入/资源/清理决定。

2026-09-20 02:25:01–02:26:17 UTC，两命令实际all旧open7/0、default原custody7/0，共14次通过。logs SHA731d259f3b536b0946d616e7f88a43e1cd46db4d081d9867a68b7a93131f52c7、4296bef1e62556fcad2dde32a36d9ae9372e9999fef27d9522f68b30194950cb，见l1e-w-pressure-fixture-*。Reviewer独立核源/原logs并0/0/0，仅接受默认fixture characterization；新pressure行为尚未测试。

实际461源（仅新增获批pressure叶），父16166、runtime1275、relay747、open657、pressure127；settlement518不变。新增观察字段尚无业务case读取时的dead_code保持可见，不加allow。下一仅在pressure叶增加真实bytecap/独立32MiB/追加deadline测试；实际oracle和新的意外失败仍按stop rule处理，不把本包当完整W接受。


并行只读F/S1/cold完整实现分配已一次收口，见l1e-f-cold-capacity-audit.json：当前runtime1275，S3后估929–939，余W15–45、F430–630、cold80–140，合计1454–1754（包含之前漏计的cold，仍仅估计）。当前pre-F W继续；真实W及S3纯Move后、首次F行为写入前必须整体关闭1500结构风险。F分配已计入R3复用，不可二次抵扣；父约121行cleanup删除不抵runtime额度。W/F共享同turn32预算，stop也不能两个32 scanner并跑；D16须运行中实际join child并发布终态避免等待环；supervisor不强持root。S1 lease沿用持久custody直到实际JoinHandle消费/资源处置，不能由futureDrop提前释放。


### W真实byte pressure与追加deadline — 局部接受

仅pressure叶127→409（+282），其余460源及原7个body保持；同129真实TCP/原session/原DATA loop。RX4097避免环绕把2字节假拆成1；actual recv记录连续slice2/limit1/consume1/RX2→1，在真实full分支反复服务仍原尾5A且read_calls保持1。fresh原128个SocketCtx队列独立求和33554432，每项262144、原handle/epoch全对应；所有原20s deadline向量保持。32MiB仅持久pending队列，原同步scratch≤256KiB和socket buffers另计，不作RSS或吞吐结论。

Tokio显式虚拟时钟下原选定Worker在D−1ms未完成、跨过D后实际JoinHandle完成但原loop尚未消费，W仍128；恢复实钟后的原stop实际join全部128、slot/live/announced归零，128guards、有界TimedOut首错，cleanup同结论。所有有限driver步骤要求fresh snapshot generation/fullcalls增长，不能以循环次数替代真实原loop进展。

2026-09-20 02:38:45–02:39:30 UTC，exact1/0及open prefix8/0，共9次通过、当前8个唯一open测试，零意外失败；log SHA78d35518e74edb72307ca557523c171bea45822ddd6a9d2795341254a2282a76、ef2fa30765356f3491c390997209a8133df197758fe8f29db040662d1cc0e0be。l1e-w-bytes-plan/source/gates与两原log保存；同一Reviewer独立0/0/0见l1e-w-bytes-independent-review.json，根02:46:41Z再次逐461源hash/bytes/lines/path核实无漂移，HEAD64f027fc/978stage逐字保持。

容量runtime1275、父16166、relay747、open657、settlement518、pressure409。下一个共享fixture/actual-device观察接缝先green-to-green，再验证success Retained W/Incomplete、poll/Drop panic和独立nonWorker/live失配；pressure继续FIN/stale/cheap/32-1/DNSUDP/其他pending TCP。完整pre-F W仍须相关/治理/独立接受；S3、F/S1、cold startup与N1恢复未执行，不把本局部接受升级为完整W或TUIC恢复。


### W settlement默认fixture — 局部green-to-green接受

同一PendingOpens新增默认不变的ControlledError/可选success、poll panic、取消Drop panic行为；默认原ordinal/gate/guard/ConnectionRefused及unknownErrorDrop保持，原8个body逐字不变。runtime仅cfg(test)字段与实际device.poll_shutdown Ready后观察调用，实现在外部原custody测试叶；不是设备Drop或整个session clean证明。runtime1279、relay754、open693，settlement518/pressure409未动，461源清单记录在l1e-w-settlement-fixture-source.json。

2026-09-20 02:51:02–02:52:27 UTC，all旧open8/0、default原custody7/0，共15次执行通过。logs SHA916de89cd867c9c7f5f2404fff9b28873f311f2911e1a697ec767619de130c3d、eb8153e4f3c0637edca44d9b2ed85d90d749a6ca997cbc9c073906e1a5b4e489；l1e-w-settlement-fixture-plan/gates/archive保存，同Reviewer独立0/0/0见对应independent-review.json。仅接受默认fixture保持；新success/panic/库存行为下一包执行，不能把此15次当新行为验收。

新retained/quarantine case即使首次observe_shutdown返回Incomplete也须持续同一saved driver，独立查其余Worker实际join与device真实Ready。异常capture后的fallback同样须有界驱动到actualWorker0，再保留原诊断并清理，不能以原genericcleanup的guard归零代替句柄消费。


### L1e-F4：新panic table意外失败 — R4提案前的冻结历史

settlement v1仅新增257行到775，源码审查发现nonWorker非法bit两row缺少同root其他actualWorker并存；该版未执行，原source/tar/plan和l1e-w-settlement-cases-v1-source-review.json保留。v2补同根slot1真实Worker/live2，两row预2slots/1Worker/live2/ann1/drop0→后0/0/0/1/1；child787≤800，原518行前缀及旧8个body保持。仅此测试叶变化，461源无漂移，独立预审0/0/0后才执行。

2026-09-20 03:05:50–03:06:15 UTC，两命令：success exact1/0，随后panic table exact0/1 exit101，runner立即停。成功实际原loop Generic结果Retained；第一次Incomplete时仍有127Worker，继续同saved driver后只余原成功slot1/live1、实际Worker0、guard128、device真实Ready，peer读取仍Pending。该一项通过不推广为完整包/成功F安装。其log SHAbdaeb38c57adb41c56d239ba73b47ca9b294badb2c1736fe94b05a0c1d00ae6b。

失败在PollPanic首行child679：before slots/workers/live128/ann0/drop1；原句柄实际join后slots/workers/live/ann0、guard128且deviceReady，立即读session.state得到Stopping，预期DeviceFailedSettled。settle_workers只承诺两个组件事实；原32槽扫描可能还有尾段、outerdriver尚未Ready。fallback继续同driver后DeviceFailedSettled且无timeout，但不覆盖原断言失败。确切最后一个Worker所处chunk未插桩，source允许此调度；无丢join/生产wake缺陷的证据。boundedfailure后续assert未到达，CancelDropPanic行、illegal exact、prefix11均未执行。失败log SHA96d1ba092dc1ca328e2daa8bf4deee9a94f401e0e5643b6ad73688873ebaa90e。

原gates/source/tar/log与l1e-w-settlement-failure.json保留；根03:08:50Z及Reviewer独立核461源hash/bytes/lines/path无漂移，HEAD64f027fc/978stage逐字不变，无Cargo/rustc/peer，见l1e-r4-stop-verification.json。Writer已停写；current failure取代已局部修复R3为当前stop，全部旧证据不删。

R4精确提案l1e-r4-proposal.json SHAdb89ed2a85e647053f645254f188bff4697e0821f9b0d06f1b1c731788db08f9，独立proposal Approve0/0/0见l1e-r4-independent-review.json。停表当时它尚未获用户授权或实施；随后用户确认并完成的结果在紧随其后的L1e-R4节记录。提案仅修改该panic函数，在原actualzero断言后保存Some原shutdown_deadline，有界await同session.observe_shutdown并断言Some(DeviceFailedSettled)及deadline不变，保原state和全部phase/resource/epoch/opaque/text/additional/destructor断言；共享helper/production不改。预计787→797–798，硬800，超则停，不能压缩/减assert。顺序失败panic exact1→illegal exact1→success exact1→prefix11，每命令前一绿后才跑；任何下一失败再次停并先确认具体方案。firstPass元数据早稿误写--已修成完整FQN，旧failure/proposal-v1字节及SHA3acaf8ae…保留，该更正零Rust/零测试。

该时点只请求这一次R4 repair确认；N1/S1/S2/S4/S5与条件S3已有授权保持。完整pre-F W、S3、F/S1、cold入口及TUIC恢复未接受，无commit/push/外部/P3/N2–N5。后续R4本地接受不改变这些未接受范围。

### L1e-R4：outer saved-driver terminal observation — local acceptance

用户于2026-09-20 05:17:54Z确认精确R4提案。唯一Rust改动保留在`compatibility_open_settlement.rs`的同一panic-table函数：在既有actualWorker=0和device Ready事实后，保存原`shutdown_deadline`，在既有五秒界内await同一saved session的`observe_shutdown`，要求`DeviceFailedSettled`并验证deadline未被重置；原有phase/resource/epoch/opaque/text/additional/destructor断言全部保留。461份Rust源码中仅该叶787→794（+7，≤800），其余460份不变；HEAD `64f027fc`、978项index和R4归档均精确冻结。

独立Reviewer先做pre-Cargo 0/0/0，再在每条命令之间复核。2026-09-20 05:54:11–05:58:32 UTC，panic-table exact1/0、illegal-inventory exact1/0、success-retention exact1/0、open-custody prefix11/0依次通过，合计14次执行通过、0失败；11是prefix唯一测试数，不能误写成14个唯一测试。最终Reviewer结论`AcceptedLocally` P0/P1/P2=0/0/0，见`l1e-r4-final-independent-review.json`；最终log/gates SHA分别为`f3d5b5bfdb54ace9286b1ed8eb5e59668f21a665886d27b007bcdde9888349d3`和`4997376a0da77fb4f96ed8bcb5d2695d75e0f2213114d0306684df6f96b977d7`。

此结论只关闭R4测试观察缝：actual worker join和device Ready不等同于outer saved driver已终态；继续同一个driver可在不改deadline的情况下完成其尾部。L1e-F4失败log、v2冻结源、提案、stop verification和所有先前证据保持原样。它不接受余下pre-F W压力矩阵、条件S3、F/S1、cold owner、生产切换或任何外部/TUN/VPS/WAN/M2/P3/N2–N5行为。下一门是只读冻结并独立预审完整pressure矩阵的容量和真实可达性，不能先写测试或以压缩/新路径绕开800行限。

### L1e-R5 — S6-MoveLayout proposal reviewed, awaiting user scope confirmation

R5 的写前停止保持原样：461 源、HEAD64f027fc、978 index entries、pressure409/800 与 parent693/800 均未因本文档阶段改变。三份只读审计先证明完整 W 矩阵没有可信单叶容量，也没有原 global_rx/D4/dirty 的可观察面；这不是运行时失败或 repair。

随后冻结的 S6-MoveLayout 只请求两个纯 Move 和三个新 Rust 路径：open fixture leaf、legacy_event_loop/mod.rs、legacy_event_loop/relay_events.rs。root 继续独占 select、所有 Sleep、sender、socket/TUN/dirty/session 生命周期；栈上 lane 仅保存 global_rx、原 handle-only D4 表、gap state 和 pause bit。event handler 的临时两组借用不得逃逸；select 顺序、D4/credit/dirty/rearm、timer reset、容量和 33 个 allowance 都保持。fixture child 的 parent-private imports、成员可见性和 observe_blocked 的 crate-level target 也已精确冻结。

独立 Reviewer 先提出 lane/timer、容量/borrow 和 fixture visibility 三类文档 P1；它们全部在提案中收敛，最终 P0/P1/P2=0/0/0，只批准 proposal，不批准 Rust。proposal JSON SHA ecd8f07a5bfefb94dea39d6c5b027221cfa5586db74007a61a22076c67951066；完整说明与 review 分别见 2026-09-20-n1-r5-event-owner-move-layout-proposal.md 与 l1e-r5-s6-movelayout-independent-review.json。此阶段未写 Rust、未运行 Cargo/rustc/测试，且 bridge、stale tracer/repair、cheap W128、FIN、reap、DNS/UDP、公平、S3、F/S1 和完整 W 均未获授权。下一步仅等待用户明确确认 S6；确认后仍先 fixture green-to-green，再 event Move。

## revision-5 G0 — no-F legacy rearm gateway，AcceptedLocal

在用户已批准 G0 范围及后续确认的一行 `legacy_event_loop/mod.rs` scope extension 内，Writer 新增 private `flow_custody/rearm.rs` 并将物理 rearm 收敛到该模块：private `request_close_or_rearm` 独占 no-F `LegacyRearmAuthorization` 的 mint，private sink 独占 abort、状态清理、fake-IP release、epoch 增加和 relisten。实际 handshake、periodic reaper、remote payload、deferred close、owned actions 和 `RelayEvent::Closed` 经 `RearmGate::original_loop` / `*_from_loop` 进入；`ReapPlan` 在任何重置写入前只读 inspect，先经 gateway 才 apply/execute。

private custody leaf 的 6 项测试全部通过：真实 current `Closed` 经原 loop bridge rearm 后保持 listener/epoch/empty-visible-state，并由同一 bridge 的旧 epoch `DataReady` 锁定入口拒绝；原 D16、旧 epoch queue 与 fake-IP reaper regressions 也保持。flow-custody prefix 8/8、七组受影响 legacy filter、growth 7/7、provenance 20/20、scoped `rustfmt --edition 2024 --check` 和 `git diff --check` 均 green。root ratchet 降至14531，`too_many_arguments` inventory 降至32；当前库存472 total /173 third-party excluded /299 governed。两位最终独立 Reviewer 均为 P0/P1/P2=0/0/0。

首次全仓 fmt 仅暴露既有不相关 dirty `owned_upstream` 格式 debt；保留为 stop evidence。将 `handle_relay_closed` 改为 test-only 后，父模块无条件 re-export 产生一次 compile stop；用户确认最小 scope extension 后，仅将该 re-export 加上 `cfg(test)` 并重跑 target green。完整机器可读结果为 `docs/current/n1-20260918-evidence/l1e-f4096-revision-5-g0-final-acceptance.json`。此阶段不接受 F1/W/F4096 压力、S4、标准 TUIC 互操作、public entry、P3/N2–N5 或任何外部 gate。

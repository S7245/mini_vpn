## 2026-11-09 去掉过宽的分类析取时，同时检查它顺带维护的状态；"多守卫"路径的变异要一次去掉全部守卫

D144：`retryable` 的 `held` 析取除了放宽分类，还顺带保证 re-probe 的 Retry 行最终会被 park 或清掉。去掉它后，终态 re-probe 留下一个未 park 的孤儿行（还会把后续 demand 的预算降为 1），第一次 GREEN 尝试因此超时；修复是在非 Waiting 分支同锁清行。A2-Q 的变异只去掉 loop 顶部的 Q 检查时仍 GREEN，因为 `reap_stats` 是第二道 Q 守卫；判别变异要去掉整个"Q 不再终态"的语义（两处一起）。负载复跑前先用 `--list` 确认每个过滤数组都非 0 匹配，并用原始 HEAD 二进制（`target/headbuild`）区分负载套件里的既有失败。另外：写含反引号的文档时 heredoc 必须加引号（`<<'EOF'`），否则 shell 会把反引号内容当命令执行。

## 2026-11-09 "慢"先归因到哪个 ticket/哪个计时器，再定修复点；第一次假设两次被证伪

D139–D141：A6-pool2 的 14s 先被归为 staged.rs 不遵守 open 期限（证伪：flow 的 open 期限是 20s），再被归为"新 demand 在 Node 已 Waiting 时才铸出"（证伪：两个 demand 都在检测时铸出，当时 held=(0,0)）。真正起作用的是在 `fail()` 里打印**所观察 ticket 的 Debug（index/sequence）**，加上在 mint 处打印 held 行：一次运行就看清慢 open 观察的是 aux 的原始 demand（seq 2），它排在主槽之后。教训：时延异常要按"等的是谁"（ticket、sequence、deadline）做归因，不要按症状像什么来猜；每次探针用独立二进制，并对源文件做 sha 还原。修复分成两个钩子（mint 时、分类为 Waiting 时），各配一个只让对应 tracer RED 的变异，以证明两个钩子彼此独立。

## 2026-11-09 门禁未选中的失败先在原始 HEAD 上复现，再决定归因于哪个阶段

D134：D133 认为 2 个 primary_recovery 失败是 N2 Waiting 的生命周期缺陷。用 `git archive HEAD | tar -x` 到 scratchpad，再用独立的 `CARGO_TARGET_DIR=target/headbuild` 构建（约 10 分钟，1.4G），在原始 HEAD 上隔离运行 3/3 同样失败，假设被证伪，确认是 N1 既有缺陷，只是从未被任何门选中。其余 4 个在 HEAD 上通过，说明它们由 D127 引起。这样做不需要 stash 或切分支，dirty 树不受影响。另外两条经验：(1) "等待 X 完成"的循环如果 X 依赖 `valid()`，就必须在无效时也能退出（wait_sealed）；(2) 判定"干净到期"时，用 custody 证据（UDP 代已 seal）作判据，比用"是否握过手"更合适，因为后者会把设计上就不握手的排队 demand 误判为终态。pool=2 的 Service tracer 没有业务流量时，aux 槽根本不会产生 demand（惰性重连），所以判别排队路径要靠 node 级 tracer。

## 2026-11-08 防误报 tracer 的变异先列"哪些计时器真的在这条路径上"，多数直觉变异是不可达的

D130：A3（RTT 400ms + 2% 丢包）先试了 4 个直觉变异：open 超时 300ms、QUIC idle 1s、端点 stall 100ms、重连超时 300ms，全部仍绿。原因是在持续流量下，活连接上没有任何应用层计时器会对 RTT 起反应（idle 被 3×PTO 钳住，open 不等 RTT，重连超时不管首连）。真正能区分慢路的是 Node 的握手预算（node.rs startup/re-probe 的 5s `timeout`），改成 300ms 后只有 A3 RED，A4/A5 仍绿。同理，A5 的 reset 并不在 `open_tcp_relay` 里被看到，而是在 ordered pump 的读取处。做法：写变异前先用 grep 列出常量和 timeout 的调用点，确认该路径确实会经过它；绿的变异作为"不可达"证据记下，不算判别变异。另外，修订旧 tracer 时让断言精确到"每个槽"，能逼出实现只覆盖 active 槽的缺口（F-N2b，默认 pool=2 时触发，而所有 A-test 都是 pool=1）。

## 2026-11-08 "和超时同时发生"的症状先做零扰动对照：去掉注入看症状是否仍在

D128：断网 25s 时 Q、预算残留、stop 卡住看起来都是 QUIC idle 15s 的后果，其实是 round-0 flow 的 15s close 尾期限恰好同时到期。决定性实验是"完全不断网"（E1），症状照旧；"先在通路正常时结算 flow 再断网"（E2）则症状消失。临时插桩可以在每个 `quarantined = true` 后打印 file:line 和 backtrace（每处上限 2 次），按 sha 还原，几分钟内就能定位源头。另外：zsh 的 `$F` 不按空格拆分，多文件备份/变异/还原要放在 bash 数组里，并在每步之后做 sha 校验。

## 2026-11-08 A/B 归因：预编译两个二进制、交替运行，树只在构建窗口内改动

D122–D123：怀疑 b2 引入 joint flake 时，没有改树后反复重跑。做法是给两个 arm 各 `--no-run` 构建一次并复制 lib 测试二进制，改动只存在于 B 的构建窗口内，随后立即按 sha 还原；然后用同一组 joint 参数按 ABAB/BABA 交替运行，以抵消机器负载漂移。arm 身份用一个只在一侧 RED 的 tracer 交叉确认（rx_fairness 在 B 中 20/20 RED）。判定规则（差值 + Fisher 单侧 p）在运行前冻结；结果是 1/20 vs 0/20，p=0.5，因此判为既有 flake，并写明 n=20 的统计功效有限。另外，libtest 的 `test … FAILED` 行可能被 stderr 打断，统计失败时要用末尾的 `failures:` 列表。

## 2026-11-07 变异要打在被评审假设上，而非最近的代码行；多重见证会让"局部"变异失去判别力

C4（D103）：M1 让 cleanup 放弃仍被共享的 DATA Arc，tracer 仍绿——被持有的 Arc 同时钉住下游物理见证（连接/endpoint drain），drain 照样不结算。改用评审原假设 M1p（Pending 误报为 Settled）才 RED，且由 `cleanup.is_none()` 双向检查拦下。冻结预测时把"经何机制 RED"也写下：M3 RED 走的是分类而非后继，说明 Failed 与 Unsettled 两道守卫重叠。

## 2026-11-06 修一个 seam 时按其“类”覆盖全部同源终态，别只修观察到的那个原因

D72 只给 remote_eof 加了 lane 次序守卫；同一信号路径的 remote_read_failed（RESET）有同样的越序，表现为另一测试的结算超时（D75）。探针本身改变时序（单跑 0/5），需在更高负载的 focused 集里抓失败。修复前先列出同一发送点（send_relay_reader_close）的所有原因。

## 2026-11-06 paused clock 测试"挂住且高 CPU" = 被测代码自唤醒空转的信号；先测线程 CPU 占比再定位

F5（D61）：paused clock 下测试 10 分钟不前进，`sample` 显示 current_thread 一半时间在 `park_yield`——说明 block_on 的 future 不停自唤醒，所以 auto-advance 从不发生。改真实时钟，在测试里用 `CLOCK_THREAD_CPUTIME_ID` 分段测 CPU 占比，得到 0.78（持有排空和 Q 后）和 0.28（稳态带载）。再在每个 `OpenEvent::Yield` 返回点和自唤醒点加 cfg(test) 计数器（结束后恢复并 sha 校验），一次就定位到 `poll_flows` 的 `take_control` 耗尽分支（≈2 万/s）。lldb attach 在本机会被权限阻塞，不要用。证伪探针（排空扫描预算 8→0）区分出"争用放大"和"O(4096) 每 tick 重扫"这个根。教训：新 tracer 的 paused clock 卡住时不要盲目改步进，先确认是不是产品自唤醒。

## 2026-11-06 丢包注入先探针包序列再定规则；"关闭包丢失"用时间窗黑洞而非序号

C3：先 print-only 探针两次看中继每代每方向包序（序号/大小/时刻），才发现旧代 CONNECTION_CLOSE 会因迟到的对端包被重发 2 次，且请求时刻附近的序号不稳定——按序号丢"第 k 个"不可复现。改为"请求+r 后黑洞旧代上行"（C 结算恰一往返之后），并以"对端 closed() 未触发"作为丢失真实发生的证伪者。握手丢包按固定序号有效，但要先核 PTO 探针个数（quinn 有 ae 在途时一次发 2 个探针），否则"连丢两次"实际只丢了一次。

## 2026-11-06 修订冻结的容量对抗 tracer 时，先查拥塞窗口和计时器：被丢弃的 ae 记录受 cwnd 限制

R1 后，`sent_capacity` 改用丢失的 PING 去耗尽 2048 环。在默认 cwnd 下增长约 300 就停住，对端随后 idle 超时。另外，PING 会重置 PTO，导致预先取的 deadline 失效；清空 outbound 又会吞掉已编码的 Close。规则：对抗耗尽用例要显式给足 cwnd（与 Service 测试相同的 8MiB Cubic），每轮重新读取连接自身的 loss deadline，并在清空 outbound 之前检查终态（D48）。

## 2026-11-06 "负载下偶发"先用 N 个精确副本并行复现，再查 typed 源头：F0 不是窗口问题

F0（D46）在联合门中出现 2 次，被并入 D11"慢窗口"一类。把精确测试 12 个副本并行跑（`par.sh`，load≈4.5）后，失败率升到 25/72。在 `RetainedFailure::from_io_ref` 加打标签的打印，第一轮就拿到不透明错误的文本：QUIC `ConnectionLost(INTERNAL_ERROR "sent history control capacity exhausted")`。由此确认是产品缺陷：Quinn fork 冷环被 ACK-only 记录占满，因为 App 背压下对端 ACK 排在积压之后。规则：(1) 不透明的 Io(Other) 先在共享构造处打印 Display 和 backtrace，再分类；(2) "仍在推进"和"typed 提前终止"是两个类别，不能合并；(3) 在异构核（M4 4P+6E）上，线程 CPU 时间不能当工作量单位，固定窗口的饥饿补偿不可靠，应改用进度停滞判定。

## 2026-11-06 残差先做“无载对照”再定断言：held 点实测常量≠泄漏

B7 在 held cut 时刻旧 Node meter 恒为 (2,300,428)，第一反应是"未结清"；同测去掉写入的探针得到完全相同值，且 200ms 不变 → Node 自有空闲残差，由 cut 后 Node drain 归还。断言改为与无载对照相等的常量，而非 0 或宽界。见证断言放在 held 点当场（而非事后 cleanup），否则变异会在后续阶段以别的症状 RED，掩盖被测不变式（D43）。

## 2026-11-06 顺序见证用对端真实可观察事件采样，并先实测哪一步真正在被测段内

B6 要证"Device 先于后继 Node drain"：在后继 peer `closed()`（Node 的 CONNECTION_CLOSE 到达）那一刻采样 (gate joins, drops, owner.driver_complete)。变异（Node drain 前移）RED 为 (1,0,false)：JOIN 早在 business stop 内完成，终 stop 段实际排序的是 Drop→driver_complete→Node drain——见证必须落在真正被重排的那一步上，否则变异仍会 GREEN。外部 queue 观察者 clone 只在载荷判定时临时取，不跨结算持有。证据 D42。

## 2026-11-06 共享预算压力注入：测试持有者必须在终态检查前归还，变异要打穿全部防线

B3 用协议层 filler 压后继 App 冷额：持有到 Service 完成会触发冻结 I6（`Service::run` 见 used≠0→Q），首个探针因此"发现 Q"——是测试伪迹，不是产品。规则：注入持有者在被测处置被观察到（后继首错或到达 peer）即归还。边界先用参数化探针二分实测（gap 83,766 拒/83,767 准），再写 self-check 常量。变异若只拆一道防线可能仍 GREEN（`record_io_admission` 已 `stop_session`，Node 门第二道）——换成"整体忽略拒绝"才 RED；记下纵深防御而非误判 tracer 无效。B4 复用 `GatedDevice` 包真实 Native 设备，注意 fixture 的 `ServiceProfile` 持 CA 临时目录，必须与 Service 同寿。证据 D35–D36。

## 2026-11-05 跨 family 复制 oracle 前先核时序：Generic 的"重叠"断言不适用于 Native

B1 把 Generic rebuild 测试的 `phase==Rebuilding at successor AUTH` 搬到 Native 即假 RED：Native 旧 Node 在后继创建前完全 drain（B818 时间线 node_drain_end→successor_start），客户端发 AUTH 后不等对端即发布 Running。改用服务侧确定性不变量（旧 business_complete、新 session 身份）。无生产改动的 characterization tracer 须用一次生产变异证明可失败（此处跳过 `resume_ingress`→RED，复原核 sha）。证据 D31、门 `n1-b1-gate-20261105-*`。

## 2026-11-05 preflight 冷额要由各 owner 自身布局函数组合，并做"恰好/少 1B"精确 tracer
O1：把 1B 占位换成真实启动冷额时，先让每个冷 owner 的 reservation 调用一个命名布局函数，再组合；精确 tracer（4 模式恰好成功、少 1B WouldBlock 无残留）首跑就抓到直读模式不需要额外拷贝。扩大 preflight 拒绝面会暴露旁路：已提供设备在工厂包装里被直接 drop、未 JOIN（变异 joins/drops=(0,1)）——拒绝路径要区分"私有工厂（不调用）"与"已存在设备（保留并经原 drive 处置）"。

## 2026-11-05 负载敏感观察窗：按"可用时间"计窗，而不是放宽或串行
O4：固定墙钟窗在并行联合下因线程饥饿误报。统一 helper 只把"定时器迟到中未被本线程 CPU 覆盖的部分"计入延长：空闲停滞与自身忙算仍在原界失败，暂停时钟等价原 timeout；变异（连自身 CPU 也补偿）使忙算 tracer 跑到 12× 上限。串行组实测 408s vs 70s，放弃。注意：这只覆盖 5s 观察窗；以 Service 停止截止为锚的 `timeout_at(deadline)` 后立即断言终态是另一类零容差边沿竞态（D28 首见，D29 修复）：截止后继续驱动原 run（断言不结束）直到终态发布，再做原断言；×5 压力是发现此类的必要门。

## 2026-11-05 未计费残差用尺寸 census 分解，再按"配置计数整倍数"定性
§5.3 差额修订 (a) 要求把≈249MB 残差拆成配置项。做法：测试分配器按精确尺寸记 Node 域存活对象数（>128KiB 按 2 次幂类），基线取首 Flow 前，满载时打印增量；再对主要尺寸抓首两次分配的 backtrace（在 Foreign 域内捕获、防递归）。结果 60 类每类计数都是 F/F+W/2×socket/5F 的整倍数——这是"残差只随配置增长"的最小证据，可折为每流实测常数×5/4；计费子集之和与计费斜率逐字节闭合（191,736B/flow）验证了分区。大类按 2 次幂上界报告会高估字节，归因总量仍用 live 计数。I_cfg 放独立总内存 preflight（hard+I_cfg），不放进计费 hard，否则等于缩 App 层。D25。

## 2026-11-05 整进程归因：交错夹具用分配域标签，并先看每流斜率
F4096 夹具每波交错驱动 peer、同进程 smoltcp App 发生器（与 Service 共用 PacketLink）与 Service 断言，子进程 peer 需整套 RPC 且 App 仍在进程内。改为计数分配器每分配头部记 1 字节域标签（realloc 保持、dealloc 按标签结算），peer quinn server 放独立线程+自定义 `quinn::Runtime`（驱动/定时器/UDP 都在该线程），测试线程上 peer/App 轮询包 `foreign`，其余保守记 Node；变异 RED 验证标签。结论不靠总峰值：每波打印 Node live 与 charged，斜率≈412KB/流 vs 计费≈192KB/流，几何实测的未计费自有实例（smoltcp socket 载荷 131,070B/socket 普通 vec）一项即让"计费+载荷">硬额 → 差额对分解方式稳健。"计费峰值≤上界"不等于"进程峰值≤上界"：上界只覆盖计费库存。证据 n1-a3attr-gate-20261105-*，D22/D23。

## 2026-11-05 闭合清单核验：按义务矩阵读断言，缺口常在"组合根的拒绝分支"
A1 八项各自 owner 的组件测试（冷少 1B 拒绝、typed Pressure、零 poll 取消、末 alias 才退休、Q 后 used>0）早已齐全；唯一缺口在 Service 组合根：A2 preflight 拒绝只有单元测试与"约 60 测试被拒"的副作用证据，没有断言"被拒 Service 不铸 budget、不调工厂、无 Node"。用 C/S/R/P/F/X/D/Rf 八列逐项填 path:line，空格即缺口；tracer 取精确边界（hard=reserve 拒、+1 准入）并做变异 RED。门里 Proto fork 测试并行下有间歇失败（两不同测试，单独 30/30），记 D19，不重试掩盖。证据 n1-a1-gate-20261105-*。

## 2026-11-05 放大系数要在隔离进程实测，且先分"计费内/计费外"
进程级计数分配器同时数 peer 与被测端；把对抗 peer（quinn server+UDP 扰动中继）放在重新执行本测试二进制的子进程里，父进程只剩真实 Node endpoint，峰值才可归因。每场景单独进程跑：同进程连续场景时前一场景的异步释放会让下一场景 baseline 偏低（Reorder 一度只测出 0.79MB）。先看 allocator 峰值与 meter 峰值之差：fork 已把 ≥92% 的协议栈分配计费，计费内的放大最终撞 Full→failstop，计费外且随输入增长的才是真无界点。实测唯一无界点是零长 DATAGRAM（upstream 按 payload 计窗口，0 字节免费）：max(len,1) 一处补丁即有界（D16）。unit 系数 preflight 漏了 c_conn（约 1.5–1.8MB/连接），小窗口诚实满窗即超 unit 上界（D17）。原 /tmp runner 已随重启丢失，见 scratch n1-gate.py 同格式重建。证据 n1-a2-k-20261105-*。

## 2026-11-05 制造真实 Proto 拒收须细粒度挤压 + 先静置；监视器由 NodeHost::run 启动
D12 tracer 要的是 quinn-proto 整包 Refused（端点拷贝成功、Proto 元数据计费失败），窗口仅约数百字节：粗粒度 slack（0/256/1024…）结果依赖前序步骤，3/5 命中；改 128B 步进 + 每步 2KiB 新流写入后 8/8 稳定命中（slack≈1280B）。全局挤压会拒收任何收包连接：握手后控制流量（ticket/CID）使旁路连接被拒，须先静置 500ms。Native 下 `start_udp` 被拒，端点监视器由 `NodeHost::run` 启动，tracer 须 select 驱动原 run。证据 n1-d12-gate-20261105-*（联合 653/653）。

## 2026-11-04 分层预算要核"谁拿哪个 meter"，不能只核构造点
D4 两层水位只在 Node 构造处改 `with_protocol_budget`，但 Native roles 经 `recv.receive_backing_meter()` 拿到的就是这只协议层 meter，App 侧下行 stock/冷库存因此在协议预留里计费；既有压力测试（F=2、8MiB App 层）根本到不了 App 天花板，不能判别。RED tracer：App 层恰满时 roles 冷准备吃进预留 69,392B。修法：bytes `app_tier_view()`（同 core 身份/计数，仅准入天花板不同）在 roles 一处接入。今后加准入层级时，枚举每个 meter 的实际流向（谁 clone、谁 adopt），并写"层满时恰在边界拒绝"的判别式 tracer。并行联合套件的固定 5s 恢复窗口对机器负载敏感：先关新接线做基线对照再定性（D11）。

## 2026-11-04 出口必须是有限集合；第三方栈用配置上界而非逐分配计量
A 自 10-17 起 3 周无 commit、约 4.9k 证据文件、56 次"有限签"仍 Open：出口含按规则只能保 Unknown 的历史归因（不可关闭），且"完整整链适用性"被执行为 quinn-proto/rustls/bytes/smoltcp 每个可达分配的事务化预扣（开放集合，单日 4 个新接缝），并越出 N1 合同"TLS/protocol 不作精确字节计量"。用户批准修订：自有库存精确、第三方栈按 TransportConfig 推上界+实测放大系数+启动 preflight、历史 Unknown 不阻塞。今后定义出口前先核：每项是否可关闭、集合是否闭合、是否仍在合同范围内；同类第三次接缝即触发结构收敛门，不能以逐项"有限签"绕过。见 docs/current/n1-a-exit-amendment-20261104.md。



## 共享预算下诚实上界需要分层水位，不只是 preflight（2026-11-04）

Native 下行 Bytes 在 Quinn 接收时记 Node meter，零拷贝交给 App 后不再扣费；App 背压就是预算 Full，故"Σ上界 ≤ 硬额"的静态 preflight 不能阻止诚实负载把 Quinn 推到 Full。同一预算身份内加协议层保留 P（App 层在 limit−P 拒绝，Node 用 `with_protocol_budget`），P = 2×代数×node 上界即可在读门存在时证明诚实 Quinn Full 不可达，且保住约 40 处 `same_budget`。另：上界接收项必须是 R_conn——本端打开的 bidi 流不受 peer 流数配置限制。测试用 `native_hard(app)=app+P` 让各测试 App 层精确等于原硬额。环境：`certs/dev/server-key.pem` 已于 9a90c62 移出仓库，依赖它的旧 TUIC fixture 在本 checkout 恒 NotFound（不新建 key）。

## 配置推导上界须由实际 TransportConfig 钉住（2026-11-04）

上界公式的输入若从常数复制，配置漂移后会静默描述另一个协议栈。`honest_bound` 用 `quic_transport_config` 的 Debug 输出逐字段断言（含 Quinn 未覆盖的默认值 datagram 1,250,000/1MiB、crypto 16KiB），并做变异 RED 验证。进程级计数分配器的峰值只在单独运行时可信，自测须 `#[ignore]` 后 `--exact --test-threads=1`。

## 2026-11-04 缓存重传帧与当前控制边界要分开
旧IssuedCid经loss回队后，编码不能无条件套当前较大retire_prior_to；RFC9000§19.15要求≤本帧sequence，min保同节点/重传/ACK且不倒退peer已知退休进度。真实多早期ACK+CID/STREAM矩阵共同首错定位后一次修复9/9。8/9项、slab−1/exact slab后Full、后续provider第4/8提交点一起核实际取消/非零Q；不要用单包绿或通用Some寿命裁定替代实际wire。最终685+Doc3/97/653/7/20，1304同源，详b560顶部。

## 2026-11-04 精确准备须重演已确认/丢失的线序，不用保守上界替代协议
ACK移除、ACK_FREQUENCY确认、共享RTT后loss会改变后续CID限额；原SentHistory排他临时位+标量值足以精确预演，取消保实物/未知保Q，无复制库存owner。协议错误前置，commit只核custody。CID先于ACK与反序不同，真实51拒绝/50成功和原handler差分都要验。差分还揭示PN身份必须含space：Handshake同号不能命中Data MTU probe。681+Doc3/97/653/7/20，1302同源；详b560顶部。受控早期差分不冒真实coalesced验收，A仍Open。

## 2026-11-04 先验证协议许可，再为“缺口”扩架构
同包退休后的新流额度不可能已通告；RFC9000§4.6要求拒绝超限输入。旧handler错误宽容不能自动变成“必须新增跨ID/Assembler复制事务”的合同。真实加密负例+实际MAX_STREAMS合法后继正例支持最小修复：invalid ID不能当unsupported绕回旧路径，应让入包前原validator拒绝。675+3/97/653/7/20，1299同源，详b560顶部。同流造洞应严格核解密offset/data/未重传hole，连接累计lost数不能代其证明。

## 2026-11-04 接收终态必须与数据准备共享wire顺序
RESET后STREAM按原规则忽略，而重复RESET仍核final/credit；停止FIN还会退休并复用原Box。准备必须一起预演received/local_max/shrink/next_remote，assembler仅覆盖RESET前有效帧，ACK确认发送半流同样可能触发表补充。原表/free缓存容量先持、原Box只移动；下一包复用已证不代表同包跨ID已证。新增事务提前拒绝后，旧frame处理计数不能证明到达，改核真实解密帧并断言尚未发布。证据n1-reset-lifecycle-final-*：673+3/97/653/7/20，1298同源；详细原件见b560顶部。

2026-11-04 控制组合需按真实Endpoint回复/路径生成时序捕wire：poll_transmit前处理原NeedIdentifiers才能CID+STREAM同包；迁移可同时产旧路径probe、新路径mixed和MTU probe。STOP与MAX信用共真实Send/事件；路径响应旧Vec也是事务库存，Slab前备保原16项规则。Close必须共享原最后状态/Close timer，不能prepared早return漏掉。668+Doc3/97/653同源通过，raw n1-remaining-*；下一RESET要含原Recv回收复用，不能仅补白名单。

2026-11-04 首次真实Data flight必须按整份wire收敛CRYPTO/RETIRE/NEW_TOKEN/STREAM。CRYPTO帧间会drain，不能套只插入STREAM batch；模拟原逐帧头读取/defrag并预备实物，provider仍原顺序。共享cache准备不跨TLS持锁，预持name/候选slab后在原固定槽重算FIFO/LRU，合法并发take/evict不应误判foreign。660+Doc3/97/653同源通过，raw n1-first-flight-*；其他控制/完整A仍Open。

2026-11-04 coalesced须原Connection共同持有各包准备结果；前包Handshake PN/ACK/timer不能先于后包Full。只读虚拟PN用于同包解码、原明文回退不能重新解密；描述Stock Full属于pre-Proto整datagram拒绝。TLS空write也可能推进keys，必须只读idle证明+预持实际Pool记录，未知先Q再close。真实Initial ACK需送达才选择Handshake PTO，不能把未构成fixture当生产RED。651+Doc3/Quinn97/653同源，raw n1-coalesced-*；新Data CRYPTO逐帧drain等仍Open。

2026-11-04 控制帧整包准备需复用原执行器：MAX_STREAMS每次有效递增各发事件，MAX_STREAM_DATA只在实际stream信用解除时按先前MAX_DATA/ACK窗口预备Writable或blocked；MSD还影响下一STREAM的opened/Readable。12首次Send+重复更新取消/后项Full/满额16事件差分锁真实顺序。原handler行为移动先640+Doc3，再646+Doc3/原653同源；额度和峰不变。raw n1-controls-*，不代跨space/coalesced或独审。

2026-11-04 首次接收必须区分map None、缓存Free、Open和已关闭缺槽。新Box先实际暂持稳定地址，Free保持原Box到commit；包内next_remote决定首个opened与后续Readable，反序12实例14帧需13事件。取消/后项Full退所有暂备实物。全局Full不代表本包需新容量，缓存有余量合法成功也要锁；真实事件填满另测拒绝。640+Doc3/653同源通过，exact n1-materialization-*，不代coalesced/独审。

2026-11-04 对端换钥须把所有known Full可拒实物准备置于next_1rtt_keys之前；真实provider会推进秘密，None/panic/取消不能冒回滚，原Connection guard保Q。prepare/install共原keys/timer，不添Session；真mixed包Full调用0次，解除压力原peer恢复，provider消费后fault计1次。636+Doc3/97/653同源通过，不代newstream/coalesced/独审，exact n1-key-*。

2026-11-03 DATAGRAM整包事务须一起准备真实descriptor、原FIFO逐帧eviction、零长消息通知与应用事件。ACK完成successor/发送解阻的终态事件要在操作开始前持槽，不能接收准备后才扩容。global Q不等于本地布局失证：核验真实销毁保Q1，失证才保实物。627+Doc3/Quinn97/联合653同源通过，raw n1-datagram-*；不代完整A或旧Direct归因。

2026-11-03 同流多帧：同owner准备必须计算先前帧去重、累计credit/FIN、每帧Readable事件及各次defrag间的真实容量；不能拼基于旧状态的独立插入。原AVL+包内区间覆盖仅查相关范围，原defrag触发才扫Heap；取消实退、满预算逐帧对照原算法。force_key_update会忽略未退休prev；仅发出请求的本地流也可能仍Free，fixture必须核实际状态/包形。616+Doc3/653同源绿不代DATAGRAM/key/coalesced/独审或未捕获包形的历史Direct失败，exact n1-multi-final-*。

2026-11-03 ACK/loss资源准备：整包事务可保原协议算法，以同Connection先备实际ACK/retransmit区间、事件与Recv/defrag，再原wire顺序提交。资源上界必须尊重ACK先于loss：首次有效ACK已确认的记录不会进入loss，为它们多备空树会放大成160B/流、F4096约655KB，配置未变也会破容量门。真实需求(2,1)→(2,0)后峰恢复原值，Proto603+Doc3/原联合653同源通过。pureACK与ACK+STREAM均需覆盖；key fallback原首错/剩余History义务保留，负例不能当完整恢复。exact n1-ack-root-qualified-* / b560-result顶部；多STREAM/key/coalesced与独审仍Open。

2026-11-03 接收库存：标量连续prefix不等于累计bytes_read；页槽、真实布局、协议信用分账。读/移位先准备完整实际路径，STOP/RESET/FIN/缓存退休须核原expected authority，不能只接受内部自洽foreign或丢root后重绑定。None不等于无实物，须核Heap与稀疏AVL；unknown由原Connection标Q，known Full不升级。布局用非test编译实测，Buffer56B撤回48B估算。manifest6d321bb1…ce1ce有限签；Proto584/定向17不能代原联合652/653，完整Full仍需原冻结合同选择。

2026-11-03 Leased完整原链：queue信用、实际stock loan和protocol Permit各自实退；callback seal后原Root仍须保clock直至实际queue Drop，跨预算拒绝先各owner Q再锁外quiet。Hard读交接原view可暴露后置copy Full与小分片观察上界，不能调额或凭组件绿忽略原Service；保原Legacy语义，wire按实际唯一payload区间核重传及真实cut位置。同guard已证自身移交可继承同步release核验，foreign mutable access/opaque重入必失效，不能跨turn缓存。manifest13f352ce…e054/同1256/648联合有限签，不代A/B/C。

2026-11-03 容量分区：真实Vec/map capacity配固定工具链bucket公式，扩容核旧新实物；test SocketCtx8632与production派生3040不能混用，FlowRoles内联不双计。RootComplete后空slot表仍存活；stats记录界不等总任务。实际old Pending＋newAUTH和fatal正额Q分别核，不能把固定meter上界称总RSS或候选永远有credit。有限独审manifest8f477bd2…cd320，571/7/20；Leased/B仍Open。

2026-11-03 原同步释放的审计复用须先封所有写入口：revision归不可整体替换的原锁外壳，每个DerefMut/get_mut先失效；poison/MAX回完整审计。成功审计只在同Turn复用，fullid/outerepoch/preparing/实物/Q每步新查。真实provider wholeRoot替换保原opaque反例不可用guard单测代替。n1-open-owner manifest7978591c…9d63c有限独审，原F4096无skip联合568/568；不冒全容量/RSS或普遍时限保证。

2026-11-03 原Main timer：now驱动poll与幂等pacer不需重放陈旧ticks；实际Service停poll60ms，13次过期回放→Native Skip1次，Legacy不改。原F4096可全4224release/20480JOIN/App0，却仍必须给Node原PTO/实际connection退休留时间；RootComplete不是Service clean。562/7/20与manifest193a1759…c149f仅签timer。

2026-11-03 空no-F reset：真正无库存且从未绑定的clear不能创造terminal历史；判据须同时核Source/mirror/delay与实际record len/capacity/cold、origin/ledger历史。旧有债/Native/历史终态不可归零重置。另只读epoch查询应借用大型receipt；测试观察字段可能放大复制成本，不能据debug样本外推生产收益。当前完整15秒stop仍RED。

2026-11-03 Main capture：contexts唯一handle逐一checked核actual SocketSet slot/meta/type，与原needs_socket双向计数等价，可合并同阶段遍历而不跳空/遗失Active反例或跨IO缓存。原F约3705观察窗RED已跨到F4096/W128/Cfull/TCPUDP；但20,480JOIN后仍provider0、15秒保757208385Q，不能把调度修复签物理关闭。521/7/20/slot1与manifest53af448c…e702900有限签仅核验收敛；停止诊断继续。

2026-11-03 原SendBuffer/Flow release：容量、ACK/loss与原operation队列必须整次调用先准备；known close还需原actual half Drop才能退map/witness。原Flow查询与末Arc/Box Drop锁外，同一个full-id/outer epoch/preparing资格在每个opaque后复核，字段实退后才最终ACK；first Provider Box不覆盖。budget0不证明诊断attempt仍持有的Runtime header已销毁。冷核用实际capacity/Layout/token，Q sentinel1不能替换保留metadata。最终519/570/97与7/20同epoch，manifest e2b2b757…4273a6e；完整容量/Leased/A/B/C未签。

2026-11-03 原uplink终态：物理Box销毁前须原receiver独占资格与实际Box双核，不能退款后才发现Broken。实际Pending ACK不是非法库存，按原min等待；原Packet须在首opaque前独立托管。AUTH后未data的正常stop仍须读原Session期限；normal read与Q效应同资格合并并发/重入，原Arc Drop独立托管。manifest aaf55d0b…9d0ab2，506/7/20及有限独审；不代SendBuffer/完整容量/A。

2026-11-03 原Pending IO期限：Main卡在真实flush时，外围census不能保证原Stop期限可达。原SessionEgress集中冷资格与通知，三个opaque字段分别托管、借用poll turn；到期Q保原future/selected/ACK，不伪IO或提前refund。实际未用库存退休Q1与未知保留512按实物分别验证。单poll观察不改真实cap/deadline；旧combined flags不能由新正常样本倒推。有限484/16/1及7/20，manifest28400746…72dc；SendBuffer/全容量仍Open。

当前原Source Pending教训（2026-11-03）：池无view只证明可用性。持未写grant等Quinn也会占住所有物理预算；同Producer独占原claim、实核替代库存后显式退闲块，重绑定仅用已资助库存，避免退/grow忙转。finalQ不能丢已完成Output或IO；原IO须JOIN前按原clock单次发布，JOIN只报告原序。manifest559c607b…8a96、最终449/7/20/check/diff及有限独审，不代完整容量/A。

## 2026-11-03 原writer JOIN与回调尾部各自证明

同步factory须在零poll取消前独立托管opaque字段；同一独占lease继续覆盖JOIN后receiver.close/poll，旧Permit还会触发Root通知。Work Drop、JOIN、receiver EOF、字段Drop、callback quiescence互不代证。Q可先使writer JOIN，测试须继续原fatal turn独立等Monitor实际close，不能要求原隐含同时性，也不能放宽原期限。证据：n1-uplink-writer-manifest.json；保零poll SIGABRT、裸尾RootWake RED和各epoch。

2026-11-03 原上行初始拒绝必须在原Flow结算：dirty-loop会丢process Err，上抛又误归DeviceRead/全Sessionstop。Receiving资格＋严格prephysical逆事务使Full保RX/W可退且generation不回退；nonFull准确Copy cause入已reported原rejection，事实先提交、receipt不重报。Q的Session早期限还需按完整outer slot/id回填Flow，不能留None待下轮now+15。真实第二Flow拒绝/第一QUIC继续及末0锁定隔离。manifest6a0f7013…f6cae仅初始边界，430/1原established RED非全绿。

2026-11-03 容量须沿实际组合根：原Service Roles冷备含真实552B退休future，组件测量漏项使Main/clock范围小计134488应为135040B，仍非完整Flow。逻辑escrow/Quinn窗口不覆盖Main→上行队列→coalesce/current/deferred的Vec；真实预算Full时观察socket消费和actual capacity，再独立验证交付/清理，才能定位绕账。Full禁止未资助的新分配，不泛化为禁止已有实物库存借用。

2026-11-02 Source gap：物理池末loan只返availability，metadata Full不能等抽象global退款；在原Producer控制turn显式实退同Flow闲置大块才可资助读前单调metadata。跨gap原grant可历史已读，取消需下一input未取、output written0/未finish/无pending。未Drop放弃保原实物/Q，已Drop后收口拒绝只保仍存在库存，不能伪报全未退。原Full与379联合、有限独审fdee4096…3e095；Leased/完整峰值/A仍Open。

2026-11-02 RX生命周期：冷保护只返回availability，实际inventory退休才退费；零长Shared alias也需实际唯一性证明。原Actor的STOP/PAUSE通知字段分别析构托管。Session常被Service保留，不能只把原IO guard挂到Session Drop：正常终态须显式处置、别名等待须沿原期限和有界控制节拍；未知处置保实际source/Box/资格/冷账。真实Main fatal出口、Direct backend及只读Q都须沿共同typed report，不能只接startup Observer/business返回口。RX有限签已保历史epoch；Source gap/Full当前亦有限签，不外推完整A1。

2026-11-02 Source Byte真实退休：DATA处置资格必须允许Main先释放末loan，最终库存/回调退休才最终ACK；EOF先退未用read grant，gap保原grant仍裁当前credit。外层run一次Pending不是原waiter注册证明，真实注册后无await末Drop才证明通知。None-key接收方只能收loanless，拒绝foreign原Root不依赖receiver有key。n1-source-byte-wire-manifest366fa934…8c738f有限签，原Full仍DeviceRead/WouldBlock RED。

2026-11-02 原输出事务：已准入实物的finish仍可能因Q拒绝，须借原Option完成fallible检查后才移走；assembler树/offset最后提交。JOIN与字段/callback/provider退休分事实观察，先验尾账再App消费。n1-source-admitted-output-lifecycle238通过，Source生产接线/Full仍Open。

2026-11-02 Source控制：Q拒DATA与原executor取消wake必须分开；原producer Sender的真实Drop也属于同Flow锁外处置，独占资格须保持到ctx＋Flow最终ACK。JOIN先到不等于proxy/库存已退休。真实charged输入才能隔离source mismatch；既有同5s内分别验JOIN/settled/Retiring，不延期限。n1-source-control-manifest 1cf65bae…54001有限独审，177＋原回归57/7/20/check通过；FullACK仍RED。

当前Source控制教训（2026-11-02，未签收）：安全proxy在Q时丢wake，会阻止原actor真正取消/JOIN。Source取消唤醒与DATA准入须分开，且start/stop/gate与退休共用原Flow锁外独占事务；JOIN/physical callback/最终库存仍分别证明。Session同步quiet不等于Service已执行控制turn，后者必须继承同一早期限。真实处置进展才可继续消耗原bounded control预算。证据n1-source-actor-verified-*及n1-source-q-cancel-red*，当前还有回归RED，不代Source/A通过。

当前Source冷turn教训（2026-11-02）：临时None不证明物理absent；原token必须首poll前实持原值，同Flow完整回填门先于取消析构，丢token兜底也要合并原Session最早deadline。物理Q与新源事实不同；capture明确Admission/IncompleteIo/Inventory，物理布局核验不能暗做admit。实际原地址/FW/escrow/Box与最早期限须独立验证，正charge/Q不足以代证。证据n1-flow-coldturn-*；Source生产stock仍未接、FullACK仍RED。

当前原队列关闭教训：DATA drain、池loan归还、callback seal是不同事实。先让dispatcher依据DATA事实实退held，再让原Producer退休waiter登记/重检末loan，future实Drop后才退池并seal；不得先seal而丢失末alias唤醒。主动等待资格丢失需Q，已签writer2被动末引用归还不因WeakNone改Q。证据n1-stock-retirement-manifest.json，仍非Source端到端通过。

# Current reusable lessons

- Device单冷TX（2026-11-01）：冷实物归还只恢复availability，实际Egress退休才退款；summary占槽独立。复用resize不清旧内容，必须保原zeroed语义；零长alias仍持Shared，cold及归还均需actual move/try_into_mut，失败原Bytes进同owner Uncertain并纳owned_bytes。93/62为有限生产/回归，未签Source/RX/Full。manifest02c66fdf…331e。

- 2026-11-01 冷成本/控制准入：局部N13几何不代F4096可行性；真实工厂6,818,992B/Flow且32MiB仅4个。Device先取TX token才交RX，TX逻辑空槽不证明实物控制进路；满额已有40B RX仍停，实际DATA Drop后同帧可读。MTU1500也不限制合法65535B注入。冷probe、池可用性与global退款分别证明；必要冻结合同修订待明确答复，不靠调额掩盖。manifest c4829ea1…081f仅测量/RED。


- 2026-11-01 stock/FIFO v2：反复漏quiet不能只补caller Scope。完整账本置私有ledger，适配器为兄弟；外部只得自带quiet的reject/Scope能力。跨budget的receiver/producer/pending先各Q再锁外发布，common Q不证明每个quiet。原Work、callback、后继资格和disposal turn各有独占票据，实际Bytes Drop/池可用性/全局退款分别证明。manifest e8b9c5e9…0debd；77/62/39仅组件与原链回归，Full未恢复。N13单Flow几何不得代F4096/双代成本，先量再扩接。

- typed loan准备（2026-11-01）：先reprTransparent包原Bytes，Native FIFO/private batch/segment/Main全链保类型；Raw ingress与typed slice分开，不并列双Native或DerefBytes/IntoBytes擦能力。当前封装62/52 GREEN不证明loan/stock。后续两raw pop必须取值前拒绝或保原typed，不得先减账再`.ok()`丢失败；实际Bytes Drop先于loan归还，Rust自定义Drop体早于字段自动析构。manifest74d4c36f…b0d7c。

- A1原因观测（2026-11-01）：Endpoint total同时含RX backingFull与owned admission Full；后者含未发布reservation fence，不等于物理槽满。沿原分支同量饱和计数，真实caller正反例与末原Bytes Drop才可签观测。Weak原Node采样的meter只含登记backing、不能冒cold总额或瞬时峰；cold一次MTU试分配非持久控制信用。manifest a65af821…708a只签计数/恢复，不签真双Root ACK依赖。

- hard partial取证（2026-11-01）：Legacy leased=inflight及单packet/单Refund=全部前缀不能移作Native oracle。真实pending0后必须逐域读Main实际caps/pending、ring/Coreheld、selected、joined dispatcher held/segment，再与独立queue守恒，不能反算差额；wire从实际TCP origin连续累积至partialcut。CurrentThread同步借读仅限封闭任务集，op0不证明排除并发，不授clean/退款。时钟不变不保证同poll公布Q；hard fatal用已审deadline+5观察界但绝不Ready/receipt，Legacy不变。manifestfffd7e76…80e0b有限62通过，A1真2FlowFull仍Open。

- Native cap回放（2026-11-01）：Admit不总是新cap；固定cap1会在同front_ordinal扩展live remaining。验实际receipt时新cap严格递id，扩展须live单cap/连续range尾/before匹配/checked after；Refund及最终count/sum不弱化。原Root Copy收据与test强queue observer是不同证据；后者留冷库存，末Node0前只退役测试observer，真实Root仍承担receipt。n1-hard-ordered-manifest4c138902…3f7b8有限58通过，非生产账本修复或A完成。

- hard queue观察（2026-11-01）：Byte队列裸snapshot会在hard reject_raw并Pending，验证用已有cfg(test) custody Copy。强testobserver自身保冷库存；真实Roles settled（含原收据，非只JOIN）后Copy最终custody并释放，才能要求Node0，不能赌QUIC drain会yield。Session父stop是Control先提交、driver后桥min，提前None不是冻结期限。RESET必须full open_origin(id,epoch,TcpOpen)排Main Other派生诊断。n1-hard-reset-manifest95fde717…e40b1有限48通过。

- hard loaded（2026-11-01）：同Egress returned载体已由原Session登记，外层驱动只能按私有carrier＋原Weak地址精确去重，不能按文本/kind/anyQ压掉foreign或fresh源IO。hard Flow可能早于parent按原15秒Q；Main随后返回的DeviceWrite/Other opaque是原admission诊断，须核同Flow原期限/close_reason再分类，不能冒底层DeviceIO或要求Legacy None。clean末预算0须释放测试App observer；原TLS公共入口只收测试输入，Legacy key义务不变。n1-hard-loaded-manifest cf80a6d5…513ce有限43通过，A仍Open。

- hard Direct cap（2026-11-01）：remaining0先Q，再保存固定terminal；Q后global hint无交付保证。不能从Flow旧UI原因推断未触发cap，更不能把派生Main refusal误判独立inventory坏。先原入口+admission被动因果probe，再按完整FlowIdentity只读实际emitted/untaken Closed库存；cap无原IO/Box，不为测试制造源IO。n1-hard-direct-manifest682f77b9…5e29d有限3项/全29通过。

- hard Service验收：first quiet与同步stop/deadline不保证同poll UI phase已变；核原control authority，最终再核Q/census。FatalPhysicalTurn保Main实物，provider/lane未退时五JOIN仍F1，真实静止或原deadline才census。Device Pending取消先观察真实poll入口，queue旁观者须在末预算0之前释放。n1-hard-service-manifest a690da01…a424c仅有限组合，非完整六处置。

- 原Native UDP生产组装（2026-11-01）：cold库存必须先于DATA attach/monitor，最终Source EOF与unused cold都须真物理退休，诊断shell留Arc不能占掉正常Service预算0。唯一NodeHost逻辑Drop必须即时封口，actor/observerArc不能替saved root。Source proof保typed Err到caller；bool缺witness不是第二个IO源。TaskRoot header硬字节与cold task实例资格/RSS分开证明，不能增假义务或借有限签放行A。证据 n1-udp-composition-manifest.json（f6f27749…67f48），原预算身份/ServiceUDP均GREEN，完整A/B/C未签。

- async fn内创建Drop guard覆盖不到零poll取消；需同步准入先接管原值，再把已guard对象捕获到future。Q拒绝保调用方Option不take，完成输出必须在Work实际Drop后判Q并原样交还。原callback角色需同时持有future、clone/Drop operation与runner资格，JOIN/remove不等于可复用。见n1-udp-callback-entry-{red,green}.log及final组。

- Native fragment：所有会删/换旧partial的分支共用selected authority/count/bytes预检；完成copy在全descriptor证实之后准入，单片正常直通不碰旧partial。双向全盘点只在sweep/retire；原TTL/LRU/last-writer用2000步差分锁定。证据 n1-udp-native-fragment-manifest.json（d2f0543e…41c5），仅组件有限签。

2026-11-01 双quiet：单次源事实交付与后续owner Drop是两个事件；已交付primary水位不能因parent丢失撤回，也不能跨未交付缺口前移。固定[primary,parent,primary]释放覆盖两个方向的末引用，仅reverse升级原Weak一次；原Box/Q先于剩余owner的quiet事实/原期限，getter临时Arc同样必须guard。

2026-11-01 Shared context：generic物理owner新增lane级context后，构造取消与最终析构同样属于opaque边界。cold借原Option直到全准备成功再take；context MD＋静态字段处置，Q返回原observer，禁止先try_unwrap再假Ok或重建Arc。settled是退休资格，finish成功才证明真实退款。

2026-11-01 Native组装：safe proxy自身的wake/Drop与原caller回调须分离；普通close不能争抢Producer工作资格。早期单phase证据属其epoch；当前v2以同Signal ledger的phase/operation/runner及独占票据闭退休，退款仍依实际try_unwrap/Box销毁而非Weak失效。Waiters根库存须持到Queue末引用；receipt与总物理0不能混称。原Queue首次操作与冷bind互斥，禁止旧raw waiter/reservation升级。

2026-11-01 Global credit/ring：活Flow数不界定已取消但仍在飞的旧callback；固定record资格必须覆盖future和callback两者直到结束，耗尽typed Capacity沿真实caller处理。Q核验要覆盖Granted/同Waker快路；ring stale tail不是Full证据，只有stamp加真实head全lap证据才拒绝。逻辑credit只能在pop存free stamp后退款；组件冷布局不能冒最终Core/平台头。有限签与raw见统一结果n1-global-inventory-components-*。

2026-11-01 Global source接缝：首错在真实source捕获时quiet入原Session，不能等JOIN或跨Flow扫描；ordinal与reported必须覆盖同一事实发布。原opaque IO移锁外不等于完成，原Flow须保在飞门，唯一token取消保原物/Q，完成核完整id＋原clock才清门。hard Root丢失看实际Some槽而非live计数，保原bounded库存，不假JOIN/退款。每次Pending IO重poll都须核原Q。有限签与exact见统一结果n1-global-source-first-disposal-*。

2026-11-01 Service组合：提前建立首错owner时，必须先冻结其依赖的runtime profile；后来修改config不能修复已生成的OpenCustody模式。保同一owner、把纯选择前移，已知校验Result仍沿原factory/D错误处置。测试queue Arc持真实cold布局，外部App还持末ACK包；先取数值再释放引用，才能核最终共享预算0。

2026-11-01 Main capture：当前多处来源相互一致不能证明仍是原来源；同预扣record冻结完整身份、Weak及Some/None profile，Root冷资格证明起点，直到summary+ACK完成才退。空Idle等待下一Root时不认旧epoch为活债，也不能发新资格；有实际cap/Active仍严核。真实socket fixture须同一时钟。有限签exact见统一结果。

2026-10-31 Device冷组合：source前备全库存，构造runtime不能代替最终bind runtime身份，期望MTU/PI也不能代替实际OS profile。启动异常必须回原Service控制层提交stop/期限/Q，helper自行pending会让公开状态停Starting。原failure通知只是同Control观察能力，取消不取first；borrowed guard须在Ready及opaque poll/Drop失败前先认领已有原IO，再处置future。Repeated shutdown显式Transferred，不能缺facts就造fallback IO。有限签与exact见统一结果n1-device-cold-*。

2026-10-31：单writer不代表单在飞wake；异步peer能在原wake返回前再调度同writer。Native callback须同cold owner单runner，wake_by_ref与末Arc Drop分开保原Box。终态通知本身仍可失败，不能先claim clean再通知；原borrowed shutdown捕自唤醒poll和取消Drop，唯一Tail认领＋readonly Status保重复receipt，不把Panic当BrokenPipe首IO。

2026-10-31：共享Egress不能用host平台头猜backend framing；Native须以原cold.prefix解析同IP kernel，真实write后的summary也必须锁定。Legacy storage移动先characterize其Full/summary原语义，不把原Legacy丢包成功冒Native背压。

2026-10-31：Native opaque源不要做async generator的自动析构局部；同actor显式poll状态机让原Guard先捕首错再销毁Source。typed移交后的Transferred slot不能再审空backing；PAUSE确认通知失败必须主动Ready终止，不能依赖已封通知的下一次stop。冷lane exact与固定Control/Runtime实例头分别列界。


Device Egress（2026-10-31，有限已签）：packet AsyncWrite不能用stream write_all；selected实际包必须在原Device跨await，只有整包write才summary，短写保原包/Q且不重写suffix。TxToken不能以f(empty)编码失败冒成功，generic R须在另起unwind前真实Drop。typed准备必须早于DNS解析copy/响应编码；保原IO优先于postQ，write每轮核Q；summary Drain的最后pop不等于proof/最终end_batch已提交，空batch的未知结算也同步同App Q。完整RX/pump/capture与Service仍Open，原结果拥有证据。

Main fixed storage（2026-10-31）：cold Full须在attempt.take_carrier前拒绝；roles已造但未commit时现settled依赖started/joins，不能凭flow.close冒正常退。cap1只能先checked合并，不可push再merge；原ACK按byte range，descriptor聚合不改变proof。typed事件在转slice前验Node原authority，App同budget不同meter。D16必须取出前按pending remaining限量。finite签收见STATUS，完整Device/Service/capture/global仍Open。

Service冷边界（2026-10-31）：App长期库存与各Node须分meter同budget，否则旧Node不可能归零。实际factory放入原saved driver且首SessionControl先建立；bind panic必须在借用D处catch、保D到独立disposal，不能任future展开触发第二Drop。通知单runner锁外调用，原Box/first必须先于可见Q发布，因果较晚的Service Drop才不会抢首错。仅证明外层Notify调用，不外推Tokio内部多Waker组合。有限独审通过，exact见STATUS/统一结果，Main/Device/global仍Open。

Native输出有限签收（2026-10-31）：逻辑length0不等于物理库存0，空root仍需cold双向live/free与真实reservation budget/size校验；同内容不能证明held→segment身份，须核allocation identity和slice起点。FIFO/主线程permit是独立生命期，实际DATA释放0后wake仍可能panic，原observer须保到最后permit并立即Q。硬预算不能挂在旧detached supervisor上冒完整生命周期；在唯一carrier入口冷拒不支持组合、返回原carrier，完整能力由原saved Root承接。零tests不算验收，所有原raw保留。

Ordered source/pump（2026-10-30）：冷票据覆盖source和FIFO完整物理库存，Direct不能因共用authority预检误占pump容量。原admitted cold observer必须与actual recv一起核，尤其actual=None时不能把前者Q丢掉；在返回reader前同步关门，别等producer运行。knownBudgetFull/TooLarge保typed原因沿原fail/期限/真实retire；Q后Closed不证明无prefix，应读原全槽物理库存，包含control/terminal、排除未发布reservation。cold Mutex要测首真实使用。

Response（2026-10-30）：统计复用槽而非调用次数，一个RecvState串行一块scratch即可。预分配Vec不是准入能力，Native输出须借同meter typed owner，foreign/input-bound先于clear/RNG/clock；真实小包分段界不能简写L−2。无pacing不能假设lease会代Q，原Endpoint MutexGuard不是Connection StateLock；实际receive scope要同次保panic/Q。cold headroom须在所有实际cold owner创建后同共享预算核验，局部batch+copy不证明完整Root可接收。exact-cold拒绝＋cold+copy真wire成功、同ptr不同DCID/token，分别防无法接收的成功构造与旧内容假复用。

Pacing（2026-10-30）：fixed容量必须包括detached正额旧lease，而非仅attached数。Full要在真实Path/key/timer变更前拒绝，释放一个历史lease后同caller必须成功。guard把retired Waker移锁外仍不够：selected Waker也须在guard析构前进入同owner安全托管，否则旧Drop panic导致selected裸Drop二次SIGABRT。opaque回调与displaced Path移到完整commit之后；不同now的精确deadline才能证实先后，Some不足。平台Mutex首次取锁可能分配，发布前冷初始化；0alloc需实际计数。未知poison/lease/终态处置同原meter Q，最后物理引用不准洗成0。

Fanout（2026-10-30）：reserved capacity不是receiver生命周期锁。原票据必须持真实lane直到同Root与所有目标槽固定提交，解锁前disarm；独立数目相等＋key/实际lane唯一才能闭Catalog↔batch两向库存。所有opaque资源（尤其首查询前的candidate）都由同事务custody安全处置，Root/lanes外wake和Drop；展开中保原payload/Q，不再wake制造二次SIGABRT。正常Full与物理损坏区别：前者无prefix正常取消，后者原authority Q。Native不可达fallback须用库存不变量和入口拒绝锁死，不能只靠当前调用习惯。详见统一结果fanout有限签，完整目标不升格。

PreparedReply (2026-10-30, `docs/current/n1-resume-20261017-evidence/b560-result.md`): Native ordinary Endpoint control须独占实际Connection，先持接收侧admission、实际Pool锁/物理链/固定active再Root最后发布，之后仅不可失败splice/安装。原Pool.prepare摘节点前审free链；同槽旧代先Stale分类，旧Native公开入口若已吞event无退款/重试则立即Q。State锁poison只恢复作sticky Q/首错观测，所有公开变更须同fatal门；实际HMAC panic/Arc、无假ACK/末Q1证明。当前仅有限签，完整Endpoint/A仍Open。

2026-10-30 N1 previous pending/source finite accepted: bind source capability as well as target identity. A sender carrying a valid B handle/key must not settle B. Use Legacy/Awaiting/Bound states; deny unpublished fallback and validate the original Bound for every Proto/Auth message before Root. Keep socket-recovery pending inside the same fixed Live Entry, audit old-socket existence in both directions, and reject generation exhaustion before fanout/socket changes. Explicit caller probes are not naturally authenticated wire events.

2026-10-30 N1 Host/PreparedConnect finite accepted: a physically live Connection is not yet publishable. Reserve identity and the same fixed instance slot without DATA routes, prepare the actual State/poller/oneshots/future/mutex outside Root, then validate stop/socket version/lifecycle and publish Proto+Host together. Keep private-State→Endpoint lock order and spawn outside Root; construct the driver release guard before its first poll. Bind terminal observer publication qualification only after real publication; a later settled watermark cannot qualify a canceled generation.

A last field running during Rust drop glue is not proof of normal disposal: earlier field Drop may have panicked. Refuse the disposal witness during unwind or owner poison, retain the original panic payload, and keep charged Q. An unpublished pacing attachment must be absent after proven cleanup or independently pristine before removal; numerical zero after a poisoned lock is not proof. Move attachment after fallible Connection construction and validate its actual cancellation path.

Opaque constructor panic can poison the outer Host Root even when Proto correctly quarantines. One poison-aware Root acquisition must cover driver poll/Drop, last EndpointRef, configuration and cleanup observations; mark driver_lost/Q and fail closed without a second panic. Common authority Q must end the original driver with Err rather than wait for a normal terminal ACK that Q itself disallows. This is authority failure, distinct from ordinary per-connection Full/rejection.

Earlier accepted invariants remain: precharge real backing/metadata/signer layouts at their actual owner; precompute provider/Pool/cell effects before commit; check Meta↔CID/reset and Proto↔Host in both directions on cold control, leaving DATA hash/directslot. Registration and path generations differ. Proto retirement is not Host disposal; actual last references own physical refund, never uncertainty clearance.

Use true encrypted peer Close/other-connection progress, original caller clock/first/3PTO and real driver drains for lifecycle claims. Respect actual owner Layout×instance counts in fixtures; preserve Full/pressure semantics. Default peer5 is not an 8-CID negotiation, and an owner-boundary19×8 tracer must say so. Source mirrors, actual host Cargo resolution and immutable vendor-lock provenance belong to the tested source epoch.

Current boundaries, counters and next execution are in docs/current/STATUS.md, the unique n1-f1-implementation.md and the result it names. Earlier memory is preserved exactly in archive/n1-host-prepared-20261030-LEARNINGS.md; read it only for provenance. User permits autonomous diagnosed repairs; all other gates and bans remain.

- 2026-11-06 C1 (D52): when predicting a serial lifecycle time, write each component's interval from the actual timer code first, then measure components separately with independent clocks (relay first-seen, peer closed(), stage prints). The first run falsified only one term ("idle settle is local"; really one peer RTT for the live stream's terminal settlement), which a single total-time assertion would have hidden. Mutation "relay delay 0, prediction kept" proves the intervals bite.
- 2026-11-06 D51: the fork's cold sent-history quota is a PN SPAN (tombstones count), and exists only on the Native client (meter-configured) ⇒ it caps uplink rate·(RTT+max_ack_delay) ≤ 1024·P, not downlink. Check which endpoint owns a bounded structure before converting it into a throughput ceiling.

- 2026-11-07 N1 (D104–D106): audit the public call chain before building "public entry wiring" — the CLI already ran the same saved Service (`serve`→`prepare_factory`→`rebuild::drive`); the real gap was one rejecting device-factory branch plus missing evidence (hard local recovery, late-event isolation). A same-generation "detector" mutation (re-send the old fragment on the new connection) is a cheap way to prove an isolation assertion is discriminating. Gate `n1-n1-gate-20261107`.
- 2026-11-07 N1 F1: an injected "late" event is evidence only if delivery is proven before the fault — bracket it between two whole controls on the same QUIC connection and wait for the second; `Connection::close` sends nothing queued. Pair every isolation tracer with a mutation of the owning cleanup (here `Fragments::retire_generation`). Check a counter's semantics before asserting zero (`FlowObservation.retry` is credit). (D109)
- 2026-11-07 D111–D113: a missing dev key is a fixture problem. Use an in-memory rcgen CA→leaf plus a thread-local temp CA guard; don't create persisted keys. Once SynBurst ran again it showed ingress clean (drops 0) but cleanup drain RED. Check which phase failed (scenario vs cleanup) before reusing a historical RED label.

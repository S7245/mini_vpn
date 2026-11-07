# N1 续接导航

按 `AGENTS.md` 冷启动顺序阅读 `Rules.md`、[当前 STATUS](docs/current/STATUS.md)、[唯一剩余计划](docs/current/n1-f1-implementation.md)，再看 [B556–B848 统一结果](docs/current/n1-resume-20261017-evidence/b560-result.md)的当前节和对应 raw。旧编号结果仅在特定不变量需要时按 STATUS 的最小证据索引回看，不把旧“下一步”当现行门。

当前只处理N1-NATIVE固定18格的剩余联合验收。独立Reviewer完成时间、双代预算、最终提交的只读可行性审查，结论**NO-GO直接接候选**：原5秒accept/AUTH与后续DATA观察分段，B820/B830/B832高RTT证明串行超时；已批准一枚未发布无DATA候选重叠可能缩短AUTH，但不能提前DATA或保证任意RTT/PTO。进程可控backing与owned queue须分配前同硬额度；审查当时Quinn收包/defrag/ordered/source先分配后meter，endpoint↔connection双向事件通道无界且无保留drain的Full协议，Service Root又缺Node/Device最终不可失败提交。先按[唯一计划](docs/current/n1-f1-implementation.md)界定可编译共同接缝；若不可界定，停生产接线并提出合同取舍。B849/B850探索性Bytes试作已撤回，不计生产或GREEN。B842真实旧drain Pending同代探针仍RED，B835串行30/30不覆盖B830/B832或B556默认并行；固定18逐项18A/0O，联合、父23=9A/13O/1B与两guard均不提升。原5/15/PTO、首错、charged Q、代码增长及禁令保持，不跑外部门、不提交/推送。

2026-10-28 B851–B866已完成首个有限生产接缝：Bytes共享布局硬额/预算化inline与BytesMut预扣复制、Quinn真实非空UDP接收先预扣；B861真Endpoint RED→B862 GREEN，B863 Quinn3/3、B864 Bytes13/13、B865空值1/1、B866治理7/7，独审正常非满额路径无P0。**下一唯一阻塞是原RecvState预算Full会IO Err终止整个Endpoint driver**，须固定batch/meta/stride及prev/current socket身份保留，额度释放有界唤醒并让drain事件继续；然后另界Quinn双向事件队列、defrag/source及同Root提交。当前生产Node未接硬额，第二Endpoint未创建，固定18联合/父组/两guard仍开。见[唯一计划](docs/current/n1-f1-implementation.md)与[统一结果](docs/current/n1-resume-20261017-evidence/b560-result.md)。

最新B867–B876把预算拒绝分为暂时Full、永久TooLarge、Q与分配失败；独审确认永久等待的窄缺口关闭，Bytes16/16、Quinn3/3、治理7/7，raw同编号。时间/预算/提交三门复核仍签NO-GO直接生产候选：正常Full仍会杀整个Endpoint driver，双向事件节点无界，Service最终发布未与stop线性化。下一按唯一计划先真Endpoint同Node两Flow满额保留/唤醒及drain，再审双向队列、defrag/source与同Root提交；若完整边界不能成立，停接线并作已列A/B合同取舍。不把本轮GREEN倒填固定18联合。

B877–B890真Endpoint Full RED→仅单包独立退额后GREEN；独审发现每包`release(0)`假唤醒，B883 RED→B884修复，最终Quinn4/4、Bytes17/17、治理7/7、仓库外无std check通过。**结构停线：**全Endpoint Full暂停会挡后续ACK/STOP/CRYPTO与其它Flow，若退额依赖后续包即自锁；有限缓存、持续UDP、零主动丢弃及同Node进展不能同时保证。`R-HARD-BUDGET-ADMISSION`累计第三次，已完成共同控制/预算审查并停局部补丁。唯一计划列A1仅有界计数拒绝未入Quinn/Root所有权UDP包（需新合同）、A2保零主动丢弃但失去Flow进展保证。用户只批准候选重叠，未批准A1/A2；生产Node未接预算、无候选。父组及两guard不变。

**最新B891–B910：用户已批准A1压力合同**，旧段“未批准A1”仅为当时记录。Quinn Full现在只计数拒绝未入Proto/Root的UDP，移除整Endpoint Pending/waker；B898真QUIC同Endpoint两stream第一backing持有、Full计数、退额后第二精确交付、driver budget0。B902→B903 recv_buf及最大合法GRO copy冷界，B906→B907 budgeted rebind更大段数换socket前拒绝，独审无新P0/P1；最终Quinn7/7、Bytes16/16、治理7/7。下一唯一门为同共享硬额的Quinn双向事件队列+每连接不可被DATA占用的`Drained+endpoint_released`终态槽；再闭defrag/source/owned queue与同Service Root提交。生产Node未接共同预算、无候选，18联合/父组/两guard均不提升。

仓库 `/Users/liushan/Documents/Personal/Languages/Rust/mini_vpn-recovered-20260918-jIMbzz`；raw 根目录 `/Users/liushan/.codex/visualizations/2026/10/26/01a17639-85fe-71e1-8ea8-8af5b0671fb4/`。真 Service 本地测试使用仓库外有效短期证书，运行后恢复仓库原证书并删除临时 key；不要沿用过期的旧镜像。禁止 macOS TUN/VPS/WAN/吞吐/M2及提交/推送，直至当前门明确改变。

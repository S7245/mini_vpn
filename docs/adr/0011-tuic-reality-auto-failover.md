# 刀9：健康感知 TUIC↔REALITY auto-failover —— 不对称切换 + UDP 永绑 TUIC + failover 模式恒 spawn

数据面有两条 Transport：① TUIC over QUIC（主，TCP relay + UDP datagram）；② VLESS over REALITY over TCP
（抗封锁备，**TCP-only**）。刀9 让 TCP relay 在 QUIC 被 GFW 封锁/黑洞时自动切到 REALITY、恢复后迟滞切回。

决策日期 2026-06-25（刀9，分支 `claude/knife9-auto-failover`）。设计经 understand-phase research workflow
（5 路研究 + 3 路对抗式核验，brief：`docs/tech/2026-06-24-knife9-research-brief.md`）+ grill 4 裁决。

## 决策

### 1. 分层解耦：「TUIC 连接存活」≠「TCP relay 选腿」（铁律）
- 「TUIC 连接的存活/重连」= 数据面机制（`live_conn`），TCP open + **UDP send_udp 共同驱动**，不受 failover 状态机约束。
- 「TCP relay 走哪条腿」= failover 决策（带冷却/迟滞）。
- **铁律**：failover 冷却/迟滞**只约束 TCP 选腿，绝不抑制 `send_udp→live_conn` 的 TUIC 重连**。因 UDP datagram
  永久绑 TUIC（REALITY 是 TCP-only），抑制其自愈会让 REALITY 当班期间 UDP 永久死亡。
  → `FailoverUpstream::send_udp` 恒转发 tuic、不读 active_leg、不看冷却（结构性保证）。

### 2. UDP 永绑 TUIC（不随 TCP failover 切换）
- REALITY 当班期间 UDP **优雅丢弃**（`send_udp` no-op + `udp_drops++`），不降级、不塞进 TCP-only 出口。
- 与北极星②「UDP 统一 QUIC datagram」一致；UDP-over-VLESS 不在本刀（留后续）。封锁场景下宁可丢 UDP。

### 3. 不对称切换门槛（防 flap 核心）
- **down 灵敏**（坏了快逃）：
  - 快路（黑洞/连接死，`is_dead`=`close_reason` 有值）：**1 次失败即切**。
  - 慢路（连接活但流失败）：**连续 3 次切**。
- **up 迟滞**（防抖）：REALITY 当班时后台每 30s 探 TUIC（`live_conn`，非浅探）；切回需**连续 3 次成功
  且距切换 ≥60s 冷却**。门槛严于 down 快路，构成不对称迟滞。
- **不上指数退避**：只 2 条腿 + UDP 常驱 TUIC 重连，固定 60s 冷却 + 30s 探针足够（稳优先，实现更简单）。
- 数值见 spec §2.6。黑洞检测靠 `quic.rs` idle_timeout + keepalive5s。

### 3b. 检测速度（真出口 acceptance 驱动的修订，2026-06-25）
真出口 acceptance 暴露两点：① QUIC `open_tcp` 在黑洞连接上**乐观返回 Ok**（开 bi-stream + 写 Connect 头是本地操作、
不等服务端），failover 看不到失败 → 检测下限 = `idle_timeout`（连接被 quinn 判死、`close_reason` 有值，下次 open
重连失败才切）；② spec §2.3 的「QUIC 重连 5s 超时」我漏实现 → 重连到黑洞 server 受 idle 约束可阻塞数十秒，最坏 ~60s 才切。
修：
- **`live_conn` 重连握手封 5s 超时**（`TUIC_RECONNECT_TIMEOUT`）：超时 → Err、guard 仍持旧死连接 → `is_dead`=true → 切。
- **`QUIC_MAX_IDLE_SECS` 30s → 15s**（grill 裁决）：切换 ≈ idle + 5s ≈ **~20s**。不增误切——healthy 连接靠 keepalive=5s
  （3 PING/15s 窗口）永不 idle 到阈值；弱网 15–30s 瞬时中断只自愈成一次廉价重连（~1-RTT），failover 还要求「重连也失败」才触发。
- **主动黑洞探测（udp_rx 停滞）= 检测主机制（acceptance 复测 3 轮坐实的根治）**：idle/open-success 对 QUIC 黑洞**根本不可靠**——
  ① open 写小 Connect 头黑洞下**乐观成功返回 Ok** → `record_tuic_success` 不断重置慢路计数；② keepalive=5s **架空 idle**
  （PING 重置 idle 计时器，`close_reason` 实测 >80s 才来），而 keepalive **不能删**（删了每 15s 断 SSH/websocket 等空闲长连接）。
  → 用 quinn `stats().udp_rx.datagrams` 当**存活信标**：健康连接每 ~5s 有 keepalive ACK 进来（rx 单调增），黑洞连 ACK 都收不到
  （rx 停滞）。`BlackholeDetector`：rx 停滞 ≥10s（~2 keepalive 周期）→ `record_blackhole` 切 REALITY。`spawn_health_probe` =
  down（rx 停滞，3s tick）+ up（探针，30s 限速）统一任务。**~10-13s 检测、可靠、不删 keepalive、不碰数据路径、不需探针目标**。
- **备机制**（防御纵深，非主）：TUIC `open_tcp` 5s 超时（黑洞 open hang 止血）+ live_conn 重连 5s 超时 + idle 15s。
- **first-byte 健康判定**（黑洞 ~5s 即测出）= brief §2.3 方案，但需重构 spawn 数据路径 + 首字节注入；udp_rx 探测已达同等可靠且更省，**不做**。
- **✅ acceptance 已证 failover 端到端通**（2026-06-25，3 轮）：`🔀 切到 REALITY` + `🔐 REALITY 握手成功` + 切回 TUIC 都实测发生；
  检测从 >80s（idle 被架空）→ 主动探测 ~10-13s，待复测确认。

### 4. failover 模式恒 spawn 所有 open（M3 + code-review Finding 1 深修）
- M3：把昂贵的 REALITY 多-RTT 握手 spawn 出单任务 select 主循环，避免一条慢握手饿死所有 flow。
- **`FailoverUpstream::open_is_cheap()` 恒 false**（非按 active_leg 动态判）：
  - 消除「读 open_is_cheap」与「open_tcp 内再读 leg」之间的 TOCTOU（inline 分支曾可能真跑 REALITY 握手 stall）。
  - 失败模式下 TUIC open 本身也不廉价（黑洞 reconnect 阻塞），inline 同样 stall → 恒 spawn 把所有 open
    （含 down 切换的 seamless 重试、黑洞 reconnect）移出主循环。
- **纯 TUIC 默认模式**（`MINI_VPN_UPSTREAM` 未设/`tuic`）走 `TuicUpstream`（open_is_cheap=true）**仍 inline，零回归**。
- spawn 路径正确性：`conn_epoch` 防串话（进 HandshakePending +1、rearm +1，`handle_handshake_done` 先比 epoch
  再看状态）+ `uplink_buffer`（256KB 上限，握手期上行缓存、成功后按序 flush）+ `HandshakeDone` channel 回灌。
- 并发安全：switch_to_reality/switch_to_tuic 用 `compare_exchange`（恒 spawn 后 record_tuic_failure 可并发）→
  只一个 caller 真切，其余不重复触发 seamless 重试。

### 5. opt-in，零回归优先
- `MINI_VPN_UPSTREAM=failover` **显式开启**（需 TUIC + REALITY 两腿都配齐）；默认/未设 = 纯 TUIC（零回归）。
- `tuic`/`reality` 仍作强制单腿调试旁路。

## Considered / Rejected
- **per-connection 各自判健康**：单任务主循环里多跑探测、加剧 stall → 用粘滞 group 级共享状态（`AtomicU8`）。
- **指数退避到 30min**（R2 原案）：2 腿 + UDP 常驱足够，固定窗更简单更稳 → 否决（V3）。
- **mihomo「立即切回」**：会复刻其抖动缺陷（Clash.Meta #432）→ 用不对称迟滞（连续 3 + 60s 冷却）。
- **FailoverUpstream::open_is_cheap 按 leg 动态判**（TUIC 腿 inline）：留 TOCTOU + 黑洞 reconnect inline stall →
  改恒 false（code-review Finding 1 深修，2026-06-25）。
- **in-flight relay 不打断**：靠现有 `spawn_remote_relay` 独立 task 白送（等价 sing-box `interrupt_exist_connections=false`）。

## Consequences
- L2 relay idle 超时（90s 双向静默 → 退出 + shutdown）顺带入本刀（防慢/卡死上游泄漏 relay task）。
- 切换延迟：down 快路最坏 ~35s（idle_timeout 暴露 close_reason ≤30s + 1 次重连失败），通常 <15s。
- F5 KeyUpdate 密钥轮换**不在本刀**（与 failover 主链零耦合，留刀10；brief §6 有精确规范）。
- acceptance（真出口跨机：打断 TUIC 验证切 REALITY / 恢复切回 / UDP TUIC 当班通·REALITY 当班丢）待用户跑。
- 不破 ADR-0003（单 rustls）/0004（TUIC 数据面）/0008（REALITY auth）/0010（CertVerify defer）。

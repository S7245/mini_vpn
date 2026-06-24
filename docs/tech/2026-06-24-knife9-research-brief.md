# 刀9 understand-phase research brief — auto-failover + 分离TCP/UDP + M3 + L2 + KeyUpdate

> 日期：2026-06-24 ｜ 阶段：刀9 understand-phase（研究 + 对抗式核验综合）
> 本刀范围：F1 auto-failover（TUIC↔REALITY，仅 TCP relay）｜ F2 分离 TCP/UDP 上游（UDP 永留 QUIC datagram）｜ F3 M3 握手并发化 ｜ F4 L2 relay idle 读超时 ｜ F5 KeyUpdate 密钥轮换
> 原则：**系统稳定 > 代码漂亮**（trade-off 冲突一律选稳）。
> 核验优先级：互通/正确性关键处以 **V1–V3 核验结论为准**；V 推翻 R 时采纳 V。

---

## 1. 摘要（最关键结论）

1. **黑洞检测机制已就位，R2 的关键假设过时（V3 实测纠正）。** 本仓 `src/quic.rs:20-24` 早已配置 `max_idle_timeout=30s` + `keep_alive_interval=5s`（R2 误以为 quinn 默认 keep_alive=None 需新增）。结论：TUIC 在 QUIC 黑洞下会在 ≤30s 内被 quinn 稳定打成 `close_reason()==TimedOut`，**刀9 无需新增任何 transport config**，被动信号（`close_reason`）的可靠性已具备。

2. **UDP 是硬路由约束，不是软选择（三家生产实践 + F2 边界）。** UDP datagram 永久绑 TUIC，**绝不**随 TCP failover 切到 REALITY；TUIC 不可用时 UDP 优雅丢弃（沿用现有 `udp_drops` 计数）。这等价于 mihomo「UDP 节点不支持就继续向下匹配、匹配不到就丢」。REALITY 的 `send_udp` 已是 no-op，**V2 核验：合规，不 panic 不阻塞，刀9 UDP 路径不改逻辑**。

3. **failover 切换门槛必须不对称（防 flap 核心）：down 灵敏、up 迟滞。** 照抄 mihomo fallback 的「立即切回」会复刻其已知抖动缺陷（Clash.Meta #432）。最终策略：down 区分「连接死（快路，1 次重连失败即切）」与「连接活但流失败（慢路，连续 3 次）」；up 必须「连续 3 次成功 + 60s 冷却」才切回。

4. **必须把「TUIC 连接存活/重连」与「TCP relay 选哪条腿」解耦成两个概念（V3 抓出的 R1/R2 共同盲区）。** 因 UDP 永久绑 TUIC，即使 TCP 全切 REALITY，TUIC 连接也不能闲置——它仍是 UDP 唯一出口，且 `send_udp→live_conn` 会免费驱动 TUIC 自愈。failover 状态机的冷却/迟滞**只约束 TCP 选腿决策，绝不可抑制 `send_udp` 的 live_conn 重连**，否则 REALITY 当班期间 UDP 永久死亡。

5. **in-flight relay「不打断」是本仓 spawn 架构白送的（V3）。** `spawn_remote_relay` 已是独立 `tokio::spawn`，持有具体 `RelayStream`，与主循环选腿完全解耦。切换只改「新 open_tcp 走哪条腿」，在飞 relay 自然走完——等价 sing-box `interrupt_exist_connections=false`，无需额外代码。

6. **M3 推荐「完整 epoch 框架 + REALITY-only spawn」折中（稳定优先）。** TUIC `open_tcp` 复用 QUIC 连接（open_bi 廉价）可保留 inline 零回归；只把昂贵的 REALITY 多-RTT 握手 spawn 出主循环。V2 核验 R5 epoch 防串话逻辑「部分成立」，需补 5 处（rearm 清 buffer/rx、epoch 检查置于状态检查之前、uplink_buffer 上限、SocketCtx::new 初始化）。

7. **F5 KeyUpdate 规范 V1 已字节级核验四点全真（refuted=false），可直接实施。** 公式 `traffic upd`（11 字节，非 `update`）、seq 轮换归 0、收 `update_requested(1)` 必回发 `update_not_requested(0)`、**回发必须用旧 send key 封装后才换发送密钥**（时序铁律）、收 0 只轮接收不回发——全部与 RFC 8446 + rustls 一致。

8. **F5 落地比 R3 设想更省事：`AppKeys` 已暴露 `c_ap_secret`/`s_ap_secret`（已核对源码 key_schedule.rs:27-34），`RecordKeys::new` 天然 seq=0、换密钥=新建实例。** 无需改 `record.rs` 结构（只加一个轮换方法），无需改 `derive_application_keys`；改点集中在 `reality_upstream.rs`（RealityStream 持两 secret + decode_one 把 KeyUpdate loud-fail 换成 on_key_update）。

---

## 2. Failover 策略（最终推荐）

> 综合 R1（sing-box/mihomo/Xray 生产实践）+ R2（GFW QUIC 封锁机理 + quinn 信号）+ V3（对抗式核验 + 本仓代码事实）。**互通/数值以 V3 最终推荐为准。**

### 2.1 状态模型

进程级共享状态（建议 `AtomicU8` + 几个计数器）：
- `active_tcp_leg ∈ {TUIC, REALITY}`（`AtomicU8`，默认 TUIC）——**仅管 TCP relay 选腿**。
- `tuic_consec_fail`、`tuic_consec_ok`、`reality_switch_at`（切到 REALITY 的时刻，算冷却用）。

**关键分层（V3 抓出的 R1/R2 盲区，写进 spec 防误实现）：**
- 「TUIC 连接的存活/重连」= **数据面机制**，由 `live_conn()` 负责，TCP open_tcp + UDP send_udp **共同驱动**，不受 failover 状态机约束。
- 「TCP relay 走哪条腿」= **failover 决策**，带冷却/迟滞。
- 二者解耦。冷却/迟滞绝不可抑制 `send_udp→live_conn` 的重连尝试。

### 2.2 架构落点：FailoverUpstream 包装

引入 `FailoverUpstream { tuic: Arc<TuicUpstream>, reality: Arc<RealityUpstream>, state }`：
- `impl ProxyUpstream`：`open_tcp` 读 `active_tcp_leg`（O(1) relaxed load）选腿。
- `impl DatagramUpstream`：`send_udp` **恒转发 tuic**（在包装层硬绑 F2，UDP 永不随 active_tcp_leg 切换）。

这样主循环仍可单态 `U = FailoverUpstream`，签名最小改动（亦兼容 §3 的双泛型分离方案，二选一）。

### 2.3 Down（TUIC→REALITY）触发判据，OR 任一即切

区分两条路（V3 对 R1「不分快慢」与 R2「≥2 次太慢」的修正）：

| 路径 | 判据 | 阈值 | 理由 |
|---|---|---|---|
| **快路（黑洞/连接死）** | `open_tcp` 内 `live_conn` 触发重连（QUIC handshake）**失败 1 次** | **1 次** | `close_reason` 有值才会重连，重连再失败 = 连接死且重建不了，黑洞下是干净强信号，不必等第 2 次（比 R2「≥2 次」砍掉 ~5–8s） |
| **慢路（边缘劣化）** | 连接没死（`close_reason` 为空）但 `open_tcp` 后续读不到首字节 / 命中 10s open_tcp 超时，**连续 N=3 次** | **3 次** | 保留 R1 的 3 次计数防 flap，但仅用于「连接活着却用不了」的边缘场景，不与黑洞快路混用 |

- QUIC connect+handshake 超时：**5s**（R2 下限，黑洞快暴露；非 happy-eyeballs 的 250ms/2s 节奏，别混用）。
- open_tcp 超时：**维持 10s**（现状止血值，relay 建立含 open_bi 往返，合理）。
- **「成功」的定义（防 Xray #5897 浅探误判）**：握手完成且拿到应用层首字节。`open_tcp` 拿到 `RelayStream` 且能读到响应首字节才算成功——绝不能只看 socket connect 成功。
- down 方向**不要求 hysteresis**：坏了快逃，备路可用优先于稳定在主路。黑洞是稳定持续状态（GFW 180s 窗口），不是抖动源，快切不会引发 flap。

最坏切换延迟：快路 ≈ idle_timeout 暴露 close_reason（≤30s，keepalive 5s 下通常更快）+ 1 次 5s 重连失败 ≈ **最坏 35s，通常 <15s**。

### 2.4 Up（REALITY→TUIC）切回，必须迟滞防 flap

- REALITY 当班时，后台**每 30s** 一次轻量 TUIC 主动探针（`live_conn` + open_bi + 读首字节）。
- **UDP 活跃时可复用 `send_udp` 的 `live_conn` 结果，不重复探**（UDP 数据面本就在驱动 TUIC 自愈）。仅当 UDP 也静默时才需独立低频主动探针。
- 切回条件：**连续 M=3 次成功** **且** 距 `reality_switch_at` ≥ **60s 冷却**。门槛（3 次成功）严于 down 快路（1 次失败），构成不对称迟滞。
- **不上指数退避到 30min（V3 否决 R2 该项）**：本仓只有 2 条腿且 UDP 始终驱动 TUIC 重连，固定 60s 冷却 + 30s 探针节奏足够，实现更简单（稳优先）。退避留作未来优化，刀9 不上。

### 2.5 UDP 处理（F2 硬约束）

- `send_udp` 在 `FailoverUpstream` 层**恒转发 TUIC**，永不随 `active_tcp_leg` 切换。
- TUIC 不可用 → 沿用现状优雅丢弃 + `udp_drops++`（tuic.rs:843），**绝不降级 REALITY**（REALITY send_udp 已 no-op）。
- **V2 核验**：UDP 下行 select 分支（`tuic_downlink_rx`）来源端独立于 TCP 上游选择，分离/包装零风险；REALITY no-op send_udp 合规，failover 时不 panic 不阻塞。UDP 路径刀9 **不改逻辑**。

### 2.6 切换粒度

- **粘滞 group 级共享状态（`AtomicU8`），不 per-connection 重判。** 新 TCP 连接读它选腿（O(1)）。per-connection 各自判健康会在单任务主循环里多跑探测、加剧 stall。
- **在飞 relay 不打断**——靠现有 `spawn_remote_relay` 独立 task 架构白送（等价 sing-box `interrupt_exist_connections=false`）。

### 2.7 不动的现状（已就位，勿重做）

- `max_idle_timeout=30s` / `keep_alive_interval=5s`（quic.rs:20-24）——黑洞检测已就位，**R2「需新增 keepalive」假设过时（V3 纠正）**。
- REALITY 10s 握手超时（作 spawn 内兜底保留）；TUIC `live_conn` 重连（作被动信号源，加计数即可，无需重写）；UDP 丢弃语义（tuic.rs:843）。

### 推荐默认值汇总

| 参数 | 推荐值 | 依据 |
|---|---|---|
| down 快路阈值（重连失败） | **1 次** | V3：close_reason 有值 + 重连失败 = 连接死强信号 |
| down 慢路阈值（连续流失败） | **3 次** | R1 mihomo max-failed=5 收紧版 / 仅边缘场景 |
| QUIC connect+handshake 超时 | **5s** | R2 黑洞快暴露 |
| open_tcp 超时 | **10s（维持）** | 现状止血值 |
| up 探针节奏（REALITY 当班） | **30s** | UDP 静默时才独立探，否则复用 send_udp |
| up 切回阈值（连续成功） | **3 次** | Clash.Meta #432 hysteresis |
| up 切回冷却窗 | **60s** | 时间维 hysteresis |
| 指数退避 | **不上（刀9）** | V3：2 腿 + UDP 常驱，固定窗足够 |
| max_idle_timeout / keepalive | **30s / 5s（已就位，不动）** | quic.rs:20-24 |
| 探活判定 | **握手完成 + 应用层首字节** | 防 Xray #5897 |

---

## 3. 分离 TCP/UDP 上游 + FailoverUpstream 架构（F2 + F1）

> 引 R4 change-map + V2 核验。改点标 `文件:行`。

### 3.1 当前形状（要改什么）

- `src/client_tun.rs:431-440` — `run_event_loop<D, U, M>(device, upstream: Arc<U>, tuic_downlink_rx, ...)`，约束 `U: ProxyUpstream + DatagramUpstream`（同一个 U 既 TCP 又 UDP）。
- `src/client_tun.rs:342-410` — `start_tun_proxy` 经 `MINI_VPN_UPSTREAM` 二选一单态化一支，device 只在选中支被 move。
- `src/client_tun.rs:609` — `process_dirty_relay` 用 `upstream` 开 TCP。
- `src/client_tun.rs:546` — `handle_tuic_udp_uplink` 用 `upstream` 发 UDP。
- `src/upstream.rs:19-23` — `ProxyUpstream` / `DatagramUpstream` 两 trait 已分离（设计形状已对）。

### 3.2 两种改法（择一）

**方案 A（推荐，最小改动）— FailoverUpstream 单态包装**：保持 `run_event_loop<D, U, M>` 单态 `U = FailoverUpstream`，`U` 同时 impl 两 trait（`send_udp` 恒走 tuic）。签名几乎不动，TCP/UDP 分离在包装层内部完成。

**方案 B — 双泛型显式分离**：`run_event_loop<D, TCP_U, UDP_U, M>(device, tcp_upstream: Arc<TCP_U>, udp_upstream: Arc<UDP_U>, ...)`，`TCP_U: ProxyUpstream`、`UDP_U: DatagramUpstream`。`process_dirty_relay` 用 `tcp_upstream`、`handle_tuic_udp_uplink` 用 `udp_upstream`。

> 推荐 **方案 A**：与 §2.2 一致，签名改动最小，F2 硬绑在包装层一处钉死，心智负担低，契合稳优先。方案 B 更显式但接线点更多。

### 3.3 接线（start_tun_proxy）

```
let tuic = Arc::new(TuicUpstream::connect(&tuic_cfg).await?);
let tuic_downlink_rx = tuic.start_udp();            // ← UDP 下行通道来源端，独立于 TCP 选腿
let reality = Arc::new(RealityUpstream::from_env()?);
let upstream = Arc::new(FailoverUpstream::new(tuic.clone(), reality)); // open_tcp 选腿；send_udp 恒 tuic
run_event_loop(device, upstream, tuic_downlink_rx, runtime_config, metrics).await;
```

device move 一次进 `run_event_loop` 不变。

### 3.4 V2 核验结论（UDP 下行不被破坏）

- `tuic_downlink_rx`（`src/client_tun.rs:621-643` 的 UDP 下行 select 分支）**只依赖该 channel，不访问 upstream 对象**——改 upstream 类型形状对此分支零影响。
- 下行通道来源端 `TuicUpstream::start_udp()` 独立于 TCP 上游选择。即使 TCP 是 `FailoverUpstream`，UDP 下行仍由单独 `Arc<TuicUpstream>::start_udp()` 驱动。**判定：分离/包装零风险。**

---

## 4. M3 握手并发化（F3）

> 引 R5 设计 + V2 防串话核验。**V2 判定 R5「部分成立」，需补 5 处。**

### 4.1 问题

`handle_local_payload`（client_tun.rs:~1082）`upstream.open_tcp(&target).await` 在主循环单任务 select 内 **inline await**。REALITY 每条 TCP 一次完整多-RTT 握手（最坏 10s 超时），期间整个 select 循环 stall，所有其他 flow 饿死。TUIC 复用 QUIC 连接（open_bi）便宜，inline 可接受。

### 4.2 方案：spawn 握手 + channel 回程

- 主循环新增一条 mpsc：`handshake_done_rx`，事件 `HandshakeDone { handle, epoch, result: Result<RelayStream, ClientError> }`。
- `handle_local_payload` 首包路径：**不 inline await**，改为：置 `OpeningRemote` + `conn_epoch += 1`，把 `(handle, target, upstream.clone(), epoch, handshake_done_tx.clone())` 捕获进 `tokio::spawn`，spawn 内跑（含 10s 超时）`open_tcp`，完成后 `send(HandshakeDone)`，主循环立即返回处理其他 flow。
- 主循环 select 新增分支 `Some(HandshakeDone) = handshake_done_rx.recv() => handle_handshake_done(...)`：成功→建 uplink channel、flush 握手期缓存的上行包、`spawn_remote_relay`；失败→`rearm_socket`。

### 4.3 不变量

| 不变量 | 机制 | 证明要点 |
|---|---|---|
| **防串话（epoch guard）** | `conn_epoch` 每进 OpeningRemote +1，写入闭包；`handle_handshake_done` 先比 epoch，不匹配丢弃 | 迟到的老 epoch 握手结果（socket 已 rearm/重连）被丢弃 |
| **防双开（OpeningRemote guard）** | 已是 OpeningRemote 时后续上行包入 `uplink_buffer`，不再 spawn 新握手 | 同 socket 不并发触发多握手 |
| **上行顺序** | 握手期 `uplink_buffer.push`（FIFO）→ 完成后 `drain(..)` 按序 send 入 uplink_tx | Vec push/drain 保序 |
| **fake-IP 引用计数平衡** | 成功 acquire / 失败走 rearm release | 首包定 fake_ip，buffer 包不增计数 |

### 4.4 V2 核验：R5 需补的 5 处（实现 spec 必须写入）

| 位置 | 漏洞 | 修正 |
|---|---|---|
| `rearm_socket`（client_tun.rs:942-960） | `uplink_buffer` 新字段未清空 → 内存泄漏 | 新增 `ctx.uplink_buffer.clear()` |
| `rearm_socket` | `handshake_result_rx`/待收结果未清 | 新增 `ctx.handshake_result_rx = None`（或等价丢弃） |
| `handle_handshake_done` | epoch 检查与状态检查顺序不清（TOCTOU 隐患） | **epoch 比较必须置于状态检查之前**，确认本代后再查 state |
| `SocketCtx`（client_tun.rs:76-92） | `uplink_buffer` 无界增长 → OOM | 加字节上限（建议 256KB）+ 溢出丢包；R5 估算 1000 连接×5s×100Mbps≈60MB 上界可接受，但仍需硬上限 |
| `SocketCtx::new`（client_tun.rs:98-106） | 新字段未初始化 | `conn_epoch=0, uplink_buffer=Vec::new()` |

补充：`reap_dead_slots`（client_tun.rs:749-780）需把 `HandshakePending`/卡住的 OpeningRemote 也视为可回收点，回收时丢弃待收握手结果。

### 4.5 风险表

| 风险 | 缓解 |
|---|---|
| HandshakeDone channel 满（并发握手 > 容量） | 容量设 128（可升 256），监控告警 |
| uplink_buffer OOM | §4.4 硬字节上限 + 溢出丢包 |
| 迟到 task 写已 rearm socket | §4.3 epoch guard |
| epoch 溢出 | `wrapping_add`，环绕后比较仍有效 |

### 4.6 低风险替代（推荐采纳：完整 epoch 框架 + REALITY-only spawn）

> V3 + R5 一致推荐，契合「稳定 > 漂亮」。

- **TUIC 腿保留 inline `open_tcp`（零回归）**：复用 QUIC，open_bi 廉价，inline 开销可接受。
- **仅 REALITY 腿走 spawn**：昂贵的多-RTT 握手移出主循环，解决唯一痛点。
- epoch / uplink_buffer / handshake_done channel 框架照建（为防串话与正确性），只是 TUIC 分支不进 spawn。
- 取舍：两条代码路径略增维护成本，但改动面小、可审查、回归风险最低。
- 备选（更激进）：TUIC 也统一 spawn（路径一致、更整洁），但回归面更大——**刀9 不取**，留未来。

---

## 5. L2 relay idle 读超时（F4）

### 5.1 位置

`spawn_remote_relay`（client_tun.rs:~1147-1202）的 select 循环当前无读超时；慢/卡住的上游（尤其 REALITY TCP-only 手写 TLS 遇 server 卡住不返回）会让 relay task 长期挂住。

### 5.2 做法

在 select 循环加 idle 计时分支：
- 维护 `idle_timeout = Duration::from_secs(90)`（建议值），用 `tokio::time::sleep`。
- 任一方向有活动（本地→上游 write 成功 / 上游→本地 read 到 n>0）即重置计时器。
- 计时器到点（90s 双向无活动）→ 打日志、`break` 退出 relay task，随后 `stream.shutdown()`。

适用两种 transport：TUIC 双向流意义在「应用层双向无数据」的清理；REALITY 防「慢 HTTP 响应 stall」。两者都受益，**不分 transport 类型**。

> 数值取舍：90s 偏宽松保稳（长轮询/SSE 类连接不被误杀）。若与上游 idle 行为冲突可调；与 §2 的 failover 探测无关（那是连接级，这是单 relay 级）。

---

## 6. KeyUpdate 轮换（F5）

> R3 精确规范 + **V1 字节级对抗核验（四点全真，refuted=false）**。以下以 V1 为准，可直接实施。

### 6.1 精确规范（V1 VERIFIED）

**(a) 下一代 traffic secret 公式（RFC 8446 §7.2）：**
```
application_traffic_secret_N+1 = HKDF-Expand-Label(application_traffic_secret_N, "traffic upd", "", Hash.length)
```
- label = `"traffic upd"`（11 字节 ASCII，`upd` 非 `update`，中间一个空格；本仓 `expand_label` 会包成 `"tls13 traffic upd"`）。
- context = `""`（空 byte slice，**不是**对 transcript 取哈希）。
- length = 32（SHA-256）。映射：`expand_label(&secret_N, "traffic upd", b"", 32)`。

**(b) 密钥轮换后 record 序列号重置为 0（RFC 8446 §5.3）：** "whenever the key is changed" 涵盖 KeyUpdate。本仓 `RecordKeys::new` 天然 seq=0，换密钥=新建实例即满足，**无需改 record.rs**。

**(c) 收到 `update_requested(1)` → 必须回发 `KeyUpdate(update_not_requested=0)`，且先用旧 send key 封装、之后才换发送密钥（RFC 8446 §4.6.3 + rustls 字节级，时序铁律）：**
- 回发 handshake 明文体 4 字节：`18 00 00 01 00`（type=24 key_update, len=1, request_update=0）。
- 作为 TLS1.3 inner plaintext（content_type=0x16 追加尾部），用**当前/旧 send `RecordKeys`** 做 AEAD 封装成一条 application_data(0x17) record 发出。
- **封装完成后才**把发送 secret 轮到 N+1、建新 send `RecordKeys`（seq 归 0）替换。
- **铁律：B1（旧 key 封装 reply）必须先于 B2（换 send 密钥）。** 先换再封 = 用新 key 发了对端还在用旧 key 解的 record → 对端解密失败掉线。rustls `enqueue_key_update_notification`（旧 key 封装缓存）先于 `set_encrypter` 证实此序。

**(d) 收到 `update_not_requested(0)`：只轮接收方向，不回发、不动发送密钥。**

**防环：** 回发的 request_update 必须是 0（RFC：MUST NOT 在响应 KeyUpdate 时发 update_requested）。
**非法值：** request_update 非 0/1 → fatal alert（illegal_parameter / InvalidKeyUpdate）。

### 6.2 derive 要暴露的 secret（已就位，比 R3 设想省事）

- **已核对源码** `src/reality/key_schedule.rs:27-34`：`AppKeys` **已含** `c_ap_secret: [u8;32]` 与 `s_ap_secret: [u8;32]`，`derive_application_keys` 已返回（R3 担心的「需新暴露」实际已完成）。
- 接收方向轮换用 `s_ap_secret`（server→client，服务端发起 KeyUpdate 最常用）；发送方向轮换用 `c_ap_secret`（收到 update_requested 时）。**两个都得留进 RealityStream 状态**。

### 6.3 RealityStream 改点（reality_upstream.rs）

- `RealityStream` 加两个可变字段：`server_ap_secret: [u8;32]`、`client_ap_secret: [u8;32]`（每轮一次就地覆盖成 N+1）。
- `open_tcp`（~:413-431）握手完成后把 `out.s_ap_secret` / `out.c_ap_secret` 传入 `RealityStream::new`（HandshakeOutput 需透出这两个 secret——若尚未，从 AppKeys 透传即可）。
- `decode_one`（约 :118-124）当前对 content_type=22 且 body[0]==0x18 的 KeyUpdate **loud-fail** → 改为调 `on_key_update`。
- `record.rs`：**结构不改**；可加一个轮换便捷方法（`RecordKeys::update_keys(new_secret)`：expand `key`/`iv` → 新建 cipher/iv，seq 归 0），或直接在 reality_upstream 里 `RecordKeys::new(&new_key, &new_iv)` 替换。

> 注：刀9 之前 RealityStream 不保留 application traffic secret —— F5 正是要补上这个状态，才能支持轮换。

### 6.4 算法伪代码（V1 核验为准，可直接落地）

```text
// 触发点：decode_one 解出一条 record 后，inner content_type == handshake(22)
// 且 handshake body[0] == key_update(0x18) → 调本流程（替代现 loud-fail）。
fn on_key_update(handshake_body: &[u8]):
    assert handshake_body[0] == 0x18                 // key_update
    let body_len = u24(handshake_body[1..4])
    if body_len != 1: fatal_alert(decode_error); return Err
    let request_update = handshake_body[4]

    // ---- 步骤 A：总是先轮换“接收”方向（对端已轮它的发送密钥）  §7.2 / §5.3 ----
    server_ap_secret = expand_label(&server_ap_secret, "traffic upd", b"", 32)
    let new_recv_key = expand_label(&server_ap_secret, "key", b"", 16)
    let new_recv_iv  = expand_label(&server_ap_secret, "iv",  b"", 12)
    recv_keys = RecordKeys::new(&new_recv_key, &new_recv_iv)   // seq 自动归 0

    // ---- 步骤 B：仅当 update_requested 才回发并轮换“发送”方向  §4.6.3 ----
    match request_update:
        0 (update_not_requested):
            return Ok                                 // 只更新读侧；不回发、不动 send

        1 (update_requested):
            // B1: 必须先用“旧”发送密钥封装回发的 KeyUpdate(update_not_requested)
            let reply = [0x18, 0x00, 0x00, 0x01, 0x00]
            let reply_record = send_keys.seal(/*content_type=*/22, &reply)  // 旧 key, 旧 seq
            tcp_write_all(&reply_record)
            // B2: 封装完毕后才轮换“发送”方向  §7.2 + §4.6.3
            client_ap_secret = expand_label(&client_ap_secret, "traffic upd", b"", 32)
            let new_send_key = expand_label(&client_ap_secret, "key", b"", 16)
            let new_send_iv  = expand_label(&client_ap_secret, "iv",  b"", 12)
            send_keys = RecordKeys::new(&new_send_key, &new_send_iv)  // seq 归 0
            return Ok

        _ : fatal_alert(illegal_parameter); return Err
```

落地铁律：B1 必先于 B2；步骤 A（轮接收）必须先于读下一条 app record；别把 KeyUpdate 的 4 字节误并入 application_data 缓冲（V1 caveat）。

---

## 7. 开放设计决策（需向用户 grill 的）

> 每条：选项 + 我的推荐 + 理由。

### D1. failover 策略细节（数值与分层）
- **选项**：(a) 照 §2 全套（down 快/慢分路 + up 3 成功 + 60s 冷却，固定窗无退避）；(b) 更保守（down 全用连续 3 次、不分快慢，切慢但更稳）；(c) 加指数退避到 30min（R2 原案）。
- **推荐 (a)**。理由：V3 证明 down 分快慢能既快切黑洞（~15s）又防边缘 flap；UDP 常驻驱动 TUIC 自愈让指数退避无必要，固定窗实现更简单更稳。
- **必须钉死的点**：冷却/迟滞只约束 TCP 选腿，绝不抑制 `send_udp→live_conn`。请用户确认这条铁律。

### D2. UDP-when-failed-over = 丢弃（北极星② 权衡）
- **选项**：(a) failover 到 REALITY 时 UDP 优雅丢弃 + udp_drops++（沿用现状，REALITY send_udp no-op）；(b) 本刀也做 UDP-over-VLESS 让 REALITY 当班时 UDP 也能走。
- **推荐 (a)**。理由：F2 边界明确 UDP-over-VLESS 不在本刀；三家生产实践共识「UDP 是硬路由约束」；V2 核验现有丢弃语义合规、不 panic 不阻塞。北极星②「UDP relay 统一 QUIC」与此一致——UDP 永远 QUIC datagram，封锁场景下宁可丢 UDP 也不把 UDP 塞进 TCP-only 出口。
- **需用户拍板**：接受「REALITY 当班期间 UDP 不可用（丢弃）」这一可用性权衡。

### D3. M3 深度（全量核心循环改造 vs 仅 REALITY spawn）
- **选项**：(a) 仅 REALITY 腿 spawn，TUIC 保留 inline（低风险折中）；(b) TUIC + REALITY 统一 spawn（路径一致、更整洁，回归面大）。
- **推荐 (a)**。理由：TUIC inline 零回归且开销小（open_bi 廉价），REALITY 是唯一痛点；契合「稳定 > 漂亮」。epoch/buffer/channel 框架照建以保正确性。
- **需用户确认**：接受两条代码路径的小幅维护成本换回归安全。

### D4. 是否本刀全做 5 项，还是分优先级
- **选项**：(a) 全做 F1–F5；(b) 分批：先 F2+F3（架构地基：分离上游 + 握手并发化），再 F1（failover）、F4（idle）、F5（KeyUpdate）。
- **推荐 (b) 分批，但 F2→F3→F1 串在同一刀（它们强耦合：F1 的 FailoverUpstream 依赖 F2 的分离 + F3 的 spawn 让 REALITY 切换不 stall）；F4、F5 相对独立可并行或顺延**。
- 依赖关系：F2 是地基；F1 依赖 F2（包装）+ 受益于 F3（切 REALITY 不 stall）；F3 独立但是 F1 的实用前提；F4 完全独立；F5 完全独立（只碰 reality 模块）。
- **需用户拍板**：本刀范围 = 全 5 项，还是先交付 F2+F3+F1 这条主链、F4/F5 视精力。考虑「one knife per session」惯例，建议本刀聚焦 **F2+F3+F1 主链**，F4（小）顺带，**F5 单独成刀**（它是独立的 TLS 正确性工作，与 failover 主链无耦合，混在一起会撑大 diff）。

---

## 8. TDD 任务分解草案

> 每项：失败测试 → 实现。标注 离线KAT/loopback（可单元/集成测）vs 需真出口 acceptance。

### F2 分离 TCP/UDP 上游
1. **[loopback]** 失败测试：mock `ProxyUpstream`（捕获 open_tcp 调用）+ mock `DatagramUpstream`（捕获 send_udp），驱动 run_event_loop，断言 TCP 走 tcp 腿、UDP 走 udp 腿（沿用 `CapturingDatagramUpstream` 模式 upstream.rs:74-86）。
2. 实现 FailoverUpstream 包装（send_udp 恒 tuic）/ 或双泛型签名。
3. **[loopback]** 回归测试：UDP 下行 `tuic_downlink_rx` 注入仍正常（V2 断言 a）。

### F1 failover
4. **[loopback]** 失败测试：mock tuic 腿连续返回 Err（黑洞快路 1 次 / 边缘慢路 3 次），断言 active_tcp_leg 切到 REALITY。
5. **[loopback]** 失败测试：REALITY 当班 + mock tuic 探针连续 3 次成功 + 时钟过 60s，断言切回 TUIC；不足 60s 或不足 3 次不切（hysteresis）。
6. **[loopback]** 失败测试：active_tcp_leg=REALITY 时 send_udp 仍走 tuic（F2 硬约束不被 failover 破坏，V2 断言 b）。
7. 实现状态机（AtomicU8 + 计数器 + 冷却）+ FailoverUpstream.open_tcp 选腿。
8. **[真出口 acceptance]** TUIC 正常→人为打断 TUIC（kill/防火墙）→ 验证 TCP 切 REALITY 仍 HTTP 200；恢复 TUIC → 60s+ 后切回。UDP（DNS over QUIC datagram）在 TUIC 当班时通、REALITY 当班时丢。

### F3 M3 握手并发化
9. **[loopback]** 失败测试：mock upstream open_tcp 故意 sleep 5s（模拟慢握手），并发投递另一 flow 的首包，断言第二 flow 不被第一个握手 stall（主循环不饿死）。
10. **[loopback]** 失败测试（epoch 防串话）：spawn 握手→在结果回来前 rearm socket（epoch++）→ 注入迟到 HandshakeDone（旧 epoch），断言被丢弃、不装到新代 socket。
11. **[loopback]** 失败测试（上行顺序 + buffer）：握手期连发 A、B，完成后断言 uplink 收到顺序 A、B；buffer 超上限断言溢出包被丢。
12. 实现 spawn + handshake_done channel + handle_handshake_done + §4.4 五处补丁。
13. **[真出口 acceptance]** REALITY 模式下多并发 curl，验证一条慢握手不拖垮其余 flow（对比 inline 基线的 stall）。

### F4 L2 idle 超时
14. **[loopback]** 失败测试：建 relay，两端 90s 无数据，断言 relay task 自动退出 + stream.shutdown 被调；中途有活动则计时重置不退出。
15. 实现 select 加 idle_timer 分支。

### F5 KeyUpdate（独立刀候选）
16. **[离线 KAT]** 失败测试：用 RFC/已知向量或自构造，断言 `expand_label(secret, "traffic upd", b"", 32)` 链式派生正确（N→N+1）；新 recv/send key/iv 与预期一致；seq 归 0。
17. **[离线 KAT]** 失败测试（时序铁律）：构造收到 update_requested(1) 场景，断言回发 record 用旧 send key 封装（旧 seq）、且封装后 send_keys 才轮换（用一个记录封装顺序的 mock 验证 B1 先于 B2）。
18. **[离线 KAT]** 失败测试：收 update_not_requested(0) 只轮 recv、不回发、不动 send；非法 request_update 值 → Err/alert。
19. 实现 RealityStream 持两 secret + decode_one→on_key_update + record 轮换。
20. **[真出口 acceptance]** 接一个会主动发 TLS1.3 KeyUpdate 的服务端（或诱发长连接 KeyUpdate），验证 REALITY relay 在 KeyUpdate 后仍能继续收发应用数据（HTTP 200 不中断）。

---

## 9. 引用来源

Failover / 健康检查 / UDP 路由（R1, R2, V3）：
- sing-box urltest: https://sing-box.sagernet.org/configuration/outbound/urltest/
- sing-box route rule (network tcp/udp): https://sing-box.sagernet.org/configuration/route/rule/
- sing-box priority mode 请求（未实装）: https://github.com/SagerNet/sing-box/issues/4065
- mihomo proxy-groups: https://wiki.metacubex.one/en/config/proxy-groups/
- mihomo fallback: https://wiki.metacubex.one/en/config/proxy-groups/fallback/
- mihomo rules（UDP 不支持→继续向下匹配）: https://wiki.metacubex.one/en/config/rules/
- Clash.Meta #432（fallback 切回抖动 + hysteresis 请求）: https://github.com/MetaCubeX/Clash.Meta/issues/432
- Xray burstObservatory: https://www.v2fly.org/en_US/v5/config/service/burstObservatory.html
- Xray-core #5897（REALITY 握手被封却浅探报 alive）: https://github.com/XTLS/Xray-core/issues/5897

QUIC 封锁 / idle-timeout / quinn / 防抖（R2）：
- gfw.report USENIX Security 2025（SNI-based QUIC censorship，180s 黑洞）: https://gfw.report/publications/usenixsecurity25/en/
- RFC 9000 §10.1 Idle Timeout: https://www.rfc-editor.org/rfc/rfc9000.html
- quinn TransportConfig: https://docs.rs/quinn/latest/quinn/struct.TransportConfig.html
- RFC 8305 Happy Eyeballs v2: https://www.rfc-editor.org/rfc/rfc8305
- curl #19740（两 racer 都 QUIC、丢 TCP fallback）: https://github.com/curl/curl/issues/19740
- AWS circuit breaker pattern: https://docs.aws.amazon.com/prescriptive-guidance/latest/cloud-design-patterns/circuit-breaker.html
- oneuptime Troubleshoot QUIC over UDP: https://oneuptime.com/blog/post/2026-03-20-troubleshoot-quic-over-udp/view

KeyUpdate / TLS1.3（R3, V1）：
- RFC 8446 (TLS 1.3) §4.6.3 / §7.2 / §5.3 / §7.3: https://www.rfc-editor.org/rfc/rfc8446.txt
- rustls tls13/key_schedule.rs: https://docs.rs/rustls/latest/src/rustls/tls13/key_schedule.rs.html
- rustls common_state.rs: https://docs.rs/rustls/latest/src/rustls/common_state.rs.html
- rustls client/tls13.rs: https://docs.rs/rustls/latest/src/rustls/client/tls13.rs.html
- rustls record_layer.rs: https://docs.rs/rustls/latest/src/rustls/record_layer.rs.html

本仓相关文件（绝对路径）：
- `/Users/liushan/Documents/Personal/Languages/Rust/mini_vpn/src/client_tun.rs`（run_event_loop:431、handle_local_payload inline open_tcp:~1082、spawn_remote_relay:~1147、start_tun_proxy:342-410、rearm_socket:942-960、SocketCtx:76-92、reap_dead_slots:749-780、UDP 下行分支:621-643）
- `/Users/liushan/Documents/Personal/Languages/Rust/mini_vpn/src/upstream.rs`（ProxyUpstream/DatagramUpstream:19-23、CapturingDatagramUpstream:74-86、FailoverUpstream 落点）
- `/Users/liushan/Documents/Personal/Languages/Rust/mini_vpn/src/tuic.rs`（live_conn:816、open_tcp:~1047、send_udp+udp_drops:836-846）
- `/Users/liushan/Documents/Personal/Languages/Rust/mini_vpn/src/reality_upstream.rs`（open_tcp 双段 10s:413-431、send_udp no-op:435、KeyUpdate loud-fail:118-124、直连探针雏形可复用作 up 探针:444-484）
- `/Users/liushan/Documents/Personal/Languages/Rust/mini_vpn/src/reality/key_schedule.rs`（AppKeys 已暴露 c_ap_secret/s_ap_secret:27-34、derive_application_keys、expand_label）
- `/Users/liushan/Documents/Personal/Languages/Rust/mini_vpn/src/reality/record.rs`（RecordKeys::new/seal/open，换密钥=新建实例 seq 归 0，结构无需改）
- `/Users/liushan/Documents/Personal/Languages/Rust/mini_vpn/src/quic.rs`（max_idle_timeout=30s/keep_alive=5s:20-24 — 黑洞检测已就位，R2「需新增 keepalive」假设过时）

# 刀9 spec — auto-failover（健康感知 TUIC↔REALITY）+ 分离 TCP/UDP 上游 + M3 握手并发化 + L2 idle 超时

> 日期：2026-06-24 ｜ 分支：`claude/knife9-auto-failover`（从 main `a9172a0` 起）
> understand-phase 输入：[research brief](2026-06-24-knife9-research-brief.md)（5 路研究 + 3 路对抗式核验综合）
> 原则：**系统稳定 > 代码漂亮**（trade-off 冲突一律选稳）。
> grill 裁决（2026-06-24，4 项全采纳 brief 推荐）：见 §0。

## 0. 范围 + grill 锁定决策

本刀交付 **failover 主链 + idle 超时**，KeyUpdate 拆出单独成刀：

| 子特性 | 本刀? | grill 裁决 |
|---|---|---|
| **F2** 分离 TCP/UDP 上游（UDP 永留 QUIC datagram） | ✅ | 地基 |
| **F3** M3 握手并发化（**仅 REALITY 腿 spawn**，TUIC 保留 inline） | ✅ | D3=(a) 低风险折中 |
| **F1** auto-failover（**不对称** down 快 / up 慢） | ✅ | D1=(a) 全套不对称 |
| **F4** L2 relay idle 读超时 | ✅ | 顺带 |
| **F5** KeyUpdate 密钥轮换 | ❌ → **刀10** | D4：F5 与 failover 主链零耦合，单独成刀避免撑大 diff |
| UDP-over-VLESS（REALITY 当班期间 UDP 可用） | ❌ | D2=(a)：REALITY 当班 UDP 优雅丢弃（沿用 no-op + udp_drops） |

**铁律（不变量，非选项，直接采纳）**：failover 状态机的冷却/迟滞**只约束「TCP relay 选哪条腿」，绝不可抑制 `send_udp→live_conn` 的 TUIC 重连**。UDP datagram 永久绑 TUIC，是其唯一出口；抑制它会让 REALITY 当班期间 UDP 永久死亡。

## 1. 目标 / 非目标

**目标**：QUIC（TUIC）被 GFW 封锁/黑洞时，TCP relay 自动切到 VLESS-over-REALITY-over-TCP 备路并保持可用；QUIC 恢复后迟滞切回。UDP 始终走 QUIC datagram，TUIC 不可用时丢弃。把昂贵的 REALITY 握手移出主循环，避免单条慢握手饿死所有 flow。给 relay 加 idle 超时防卡死泄漏。

**非目标**：UDP-over-VLESS（REALITY 当班 UDP 仍丢）；KeyUpdate（刀10）；TUIC 腿 spawn 化（留未来）；连接复用（REALITY 每 TCP 一握手不变）；新增任何 QUIC transport config（黑洞检测已就位，见 §6）。

## 2. Failover 策略（锁定）

### 2.1 状态模型（进程级共享，`Arc<FailoverState>`）
- `active_tcp_leg: AtomicU8` ∈ {TUIC=0, REALITY=1}，默认 TUIC。**仅管 TCP relay 选腿**。
- `tuic_consec_fail: AtomicU32`、`tuic_probe_consec_ok: AtomicU32`、`reality_switch_at: AtomicU64`（切到 REALITY 的单调秒，算 60s 冷却）。
- 单调时钟 `Instant`（与 tuic.rs 同源思路，避免双时钟漂移）。

**解耦（写进代码注释防误实现）**：「TUIC 连接存活/重连」= 数据面机制（`live_conn`，TCP+UDP 共同驱动，不受状态机约束）；「TCP relay 走哪条腿」= failover 决策（带冷却/迟滞）。二者正交。

### 2.2 架构落点：`FailoverUpstream`（brief §3 方案 A，最小改动）
```rust
struct FailoverUpstream { tuic: Arc<TuicUpstream>, reality: Arc<RealityUpstream>, state: Arc<FailoverState> }
impl ProxyUpstream    for FailoverUpstream { open_tcp → 读 active_tcp_leg 选腿 + 记成败 + 切换判定 }
impl DatagramUpstream for FailoverUpstream { send_udp → 恒转发 tuic（F2 在包装层一处硬绑） }
```
主循环仍单态 `U = FailoverUpstream`，`run_event_loop` 签名几乎不动。

### 2.3 Down（TUIC→REALITY），OR 任一即切，不要求 hysteresis（坏了快逃）

| 路径 | 判据 | 阈值 |
|---|---|---|
| **快路（黑洞/连接死）** | `open_tcp` 内 `live_conn` 触发重连（QUIC handshake）**失败** | **1 次即切** |
| **慢路（边缘劣化）** | 连接没死但 `open_tcp` 失败/超时/读不到首字节 | **连续 3 次** |

- QUIC connect+handshake 超时 **5s**；open_tcp 超时 **维持 10s**。
- **「成功」定义**（防浅探误判，Xray #5897）：拿到 `RelayStream` 且能读到应用层首字节才算 ok。
- 最坏切换延迟：≤30s（idle_timeout 暴露 close_reason）+ 5s 重连失败 ≈ **最坏 35s，通常 <15s**。

### 2.4 Up（REALITY→TUIC），必须迟滞防 flap
- REALITY 当班时，后台**每 30s** 一次轻量 TUIC 主动探针（`live_conn` + open_bi + 读首字节）。UDP 活跃时可复用 `send_udp` 的 live_conn 自愈、不重复探。
- 切回条件：**连续 3 次探针成功** **且** 距 `reality_switch_at` ≥ **60s 冷却**。
- **不上指数退避**（V3 否决 R2）：2 腿 + UDP 常驱 TUIC 重连，固定窗足够。

### 2.5 切换粒度 + in-flight
- 粘滞 group 级共享状态（`AtomicU8`），新 TCP 连接读它选腿，**不 per-connection 重判**。
- 在飞 relay 不打断（`spawn_remote_relay` 独立 task 白送，等价 `interrupt_exist_connections=false`）。

### 2.6 数值汇总
down 快=1 / down 慢=3 / QUIC 超时=5s / open_tcp=10s / up 探针=30s / up 切回=连续3+60s冷却 / 退避=无 / idle+keepalive=30s+5s(已就位不动) / 探活=握手完成+首字节。

## 3. 分离 TCP/UDP 上游（F2，brief §3）
- `run_event_loop<D,U,M>` 保持单态，`U = FailoverUpstream`（同时 impl 两 trait）。`send_udp` 在包装层恒走 tuic。
- 接线（`start_tun_proxy`）：建 `tuic` → `tuic.start_udp()` 拿下行 rx（来源端独立于 TCP 选腿）→ 建 `reality` → `FailoverUpstream::new(tuic.clone(), reality)` → `run_event_loop(device, upstream, tuic_downlink_rx, ...)`。device 仍只 move 一次。
- **V2 核验**：`tuic_downlink_rx`（client_tun.rs:621-643 UDP 下行分支）只依赖该 channel、不访问 upstream 对象 → 分离/包装对 UDP 下行零风险。
- 兼容：保留 `MINI_VPN_UPSTREAM=tuic|reality` 作**强制单腿**调试旁路（force-tuic / force-reality，不进 failover），默认（未设/`failover`）走 FailoverUpstream。

## 4. M3 握手并发化（F3，仅 REALITY 腿 spawn，brief §4）

### 4.1 方案
- TUIC `open_tcp` **保留 inline**（复用 QUIC，open_bi 廉价，零回归）。
- REALITY `open_tcp` **spawn 出主循环**（多-RTT 握手是唯一痛点）。
- 主循环新增 mpsc `handshake_done_rx`，事件 `HandshakeDone { handle, epoch, result: Result<RelayStream, ClientError> }`。
- `handle_local_payload` 首包：若选中腿是 REALITY（或统一走 spawn 接缝），置 `OpeningRemote` + `conn_epoch+=1`，spawn 内跑（含 10s 超时）`open_tcp` → `send(HandshakeDone)`，主循环立即返回。握手期上行包入 `uplink_buffer`。
- 主循环 select 新分支 `handle_handshake_done`：成功→建 uplink channel、按序 flush `uplink_buffer`、`spawn_remote_relay`；失败→`rearm_socket`。

### 4.2 不变量
| 不变量 | 机制 |
|---|---|
| 防串话（epoch） | `conn_epoch` 进 OpeningRemote +1 写入闭包；`handle_handshake_done` **先比 epoch（置于状态检查之前）**，不匹配丢弃 |
| 防双开 | 已 OpeningRemote 时后续上行包入 buffer，不再 spawn |
| 上行顺序 | `uplink_buffer` FIFO push → 完成后按序 drain 入 uplink_tx |
| fake-IP 引用计数平衡 | 成功 acquire / 失败 rearm release；buffer 包不增计数 |

### 4.3 V2 必补的 5 处（实现强制）
1. `rearm_socket`：`ctx.uplink_buffer.clear()`。
2. `rearm_socket`：丢弃待收握手结果（`ctx.pending_handshake = None` 或等价 + epoch 失配自然丢）。
3. `handle_handshake_done`：**epoch 比较先于状态检查**（防 TOCTOU）。
4. `SocketCtx`：`uplink_buffer` 加字节硬上限 **256KB**，溢出丢包（防 OOM）。
5. `SocketCtx::new`：初始化 `conn_epoch=0, uplink_buffer=Vec::new()`。
- 补：`reap_dead_slots` 把卡住的 OpeningRemote 视为可回收，回收时丢待收结果。

### 4.4 风险 + 兜底
- HandshakeDone channel 容量 **128**；epoch 用 `wrapping_add`；uplink_buffer 硬上限见上。

## 5. L2 relay idle 读超时（F4，brief §5）
- `spawn_remote_relay` select 加 idle 分支：`idle_timeout = 90s`，`tokio::time::sleep`；任一方向有活动即重置；到点打日志 + `break` + `stream.shutdown()`。
- 适用 TUIC/REALITY 两种 RelayStream，不分类型。与 §2 failover 探测无关（那是连接级，这是单 relay 级）。

## 6. 不动的现状（已就位，勿重做）
- `max_idle_timeout=30s` / `keep_alive_interval=5s`（quic.rs:20-24）—— 黑洞检测已就位（R2「需新增 keepalive」假设过时，V3 纠正）。
- REALITY 10s 握手超时（作 spawn 内兜底保留）；TUIC `live_conn` 重连（作被动信号源，加计数即可）；UDP 丢弃语义（tuic.rs:843）；`spawn_remote_relay` 独立 task（in-flight 不打断白送）。

## 7. Acceptance（真出口）
- **F1**：TUIC 正常 → 人为打断 TUIC（防火墙 drop :8443/UDP 或 kill server QUIC）→ TCP 切 REALITY 仍 HTTP 200；恢复 TUIC → 60s+ 后切回。UDP（DNS-over-QUIC datagram）TUIC 当班通、REALITY 当班丢。
- **F3**：REALITY 模式多并发 curl，一条慢握手不拖垮其余 flow（对比 inline 基线 stall）。
- **F4**：建 relay 后两端静默 90s，relay 自动清理。
- 离线：所有状态机/epoch/buffer/选腿逻辑 loopback 单测全绿；clippy 0；release 绿。

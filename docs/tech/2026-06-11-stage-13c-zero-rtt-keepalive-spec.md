# Stage 13c — 0-RTT 快速重连 + 厘清两层保活 (spec)

> 范围已与用户对齐(2026-06-11):13c **收敛**为桌面能跨机真验收的两件事——**QUIC 0-RTT 快速重连**
> 与**厘清/参数化两层保活**。真·连接迁移(WiFi↔蜂窝)与电量自适应 heartbeat **强依赖移动端接口**
> (iOS NEPacketTunnelFlow / Android VpnService),归入未来「移动端就绪」stage,不在 13c。见 ADR-0004 / 13a / 13b。

## 背景:为什么 migration 不在 13c

quinn 0.10 默认开启 QUIC connection migration,我们没禁用它;但「真·网络切换」(旧接口 down、源 IP 变)只有
移动端 packet-flow 后端才造得出,桌面 `tun` 后端最多模拟端口 rebind,**证明不了移动漫游**。每刀必须能真验收
(13a/13b 的惯例),故 migration 推迟到有 iOS/Android 接口的 stage。0-RTT 桌面可完整真验收,先做。

## 目标

1. **0-RTT 重连**:TUIC 连接断开重连时,用 QUIC 0-RTT 在握手 0-RTT 即开始发数据,省一次往返。**首连仍 1-RTT**
   (无 ticket);重连/恢复(有缓存 ticket)走 0-RTT,失败自动 fallback 1-RTT(不致命)。
2. **Session resumption 缓存**:客户端持有 rustls session cache(**内存**即可,桌面够),重连复用 ticket。
3. **厘清两层保活**:明确并参数化 **QUIC keep-alive(连接层)** vs **TUIC Heartbeat(应用层 UDP 会话)** 的职责;
   去冗余——TUIC Heartbeat 改为**仅在有活跃 UDP assoc 时发**(13b 是无条件周期发,纯 TCP 也发,浪费)。
4. legacy 路径**零回归**;现有 TCP/UDP 行为不变(只是重连更快、保活更省)。

## 非目标(→ 移动端 stage / 13d)

- **真·连接迁移**(WiFi↔蜂窝,OS 接口)→ 移动端 stage。13c 不动 quinn 默认 migration,也不验收它。
- **电量/doze 自适应 heartbeat** → 移动端(需 battery/lifecycle 信号)。13c 只把周期**参数化、留接口**。
- **持久化 session ticket**(跨进程重启的 0-RTT)→ 移动端(radio sleep 恢复)。桌面内存缓存够;重启回退 1-RTT。
- **UDP datagram 的 0-RTT 早发**:datagram 不是 stream,0-RTT early data 是 stream 概念;本刀 0-RTT 针对
  握手 + Authenticate(uni)+ 首个 Connect(bi),UDP 走既有 datagram(连接建好即可发)。

## 能力确认(关键:无需 version bump)

- quinn **0.10.2** / quinn-proto **0.10.6** / rustls **0.21.12**(现状)即支持 0-RTT 客户端:
  - rustls:`crypto.enable_early_data = true`(默认 **false**,必须显式开);resumption + 内存 session cache 默认已开。
  - quinn:`endpoint.connect(...)` 得 `Connecting`,`Connecting::into_0rtt()` → `Result<(Connection,
    ZeroRttAccepted), Connecting>`。成功:立即在 Connection 上开流发 early data;失败:`Err(connecting)`,
    `.await` 走正常 1-RTT。
- **消除 ADR-0004 的版本风险**:0-RTT 不逼升级 quinn/rustls(只动 config + 重连路径)。

## 头号风险:0-RTT 阶段的 keying-material(必须互通验证)

TUIC auth token = `export_keying_material(out=32, label=UUID, context=password)`(13a)。TLS 1.3 的 exporter
在 **0-RTT(early)** 用 *early exporter master secret*,在 **1-RTT** 用 *exporter master secret*——**两者不同**。
若在 0-RTT 连接上调 `export_keying_material` 得到的 secret 与 sing-box 在 0-RTT 验 token 时用的不一致,
**Authenticate 会失败**。

应对(spec 决策,e2e 定夺):
- **优先**:Authenticate 也走 0-RTT(TUIC 的设计意图),互通 e2e 验证 token 与 sing-box 对齐。
- **退路**:若 early exporter 与 sing-box 不一致,则 **Authenticate 等握手完成(1-RTT 后)再发**,0-RTT 只省
  「连接建立」的 RTT(Connect/数据仍要等 auth)——价值打折但仍正向,且实现简单可靠(系统稳定优先)。
- 决策点放在**互通 e2e**(层 2),代码两条路都留(一个 `auth_in_0rtt` 开关),按 sing-box 实际行为定默认。

## 0-RTT 重放安全

0-RTT early data 可被网络攻击者重放。代理语义下后果有限:
- **Connect 重放** → sing-box 至多多拨一次 target TCP;inner TLS 握手失败兜底,应用层不受影响。
- **Authenticate 重放** → 重复认证,无副作用(token 绑定连接,重放到新连接也建不起会话)。
- 决策:**接受**代理语义下的有限 0-RTT 重放风险(与 TUIC/sing-box 默认一致);不对首包做额外幂等改造。

## 厘清两层保活

| 层 | 机制 | 周期(现状) | 职责 | 13c 变化 |
|---|---|---|---|---|
| 连接层 | QUIC `keep_alive_interval`(PING 帧) | 5s | 防 `max_idle_timeout`(30s)断连 | 周期**参数化**(默认不变,零回归) |
| 应用层 | TUIC Heartbeat(`[05 04]` datagram) | 3s 无条件 | TUIC 规范:维持 sing-box UDP 关联不被回收 | 改为**仅有活跃 UDP assoc 时发**;周期参数化 |

- 嵌套关系(本就合理,13c 文档化):`TUIC HB(3s) < QUIC keepalive(5s) < idle(30s)`。
- **按需 heartbeat 的实现**:`TuicUpstream` 加 `last_udp_activity: AtomicU64`(`send_udp` 每次更新);下行驱动
  任务的 heartbeat tick 时,**仅当「距上次 UDP 上行 < 阈值」才发** TUIC Heartbeat。纯 TCP 会话不发(QUIC PING
  已保连接层),省流量/电量。阈值默认略大于 idle 的安全余量。
- 这把跨任务状态共享收敛成一个 atomic(driver 读、send_udp 写),无锁、稳定。

## 模块改动

### `src/quic.rs`
- 客户端 crypto 设 `enable_early_data = true`(server config 不变;sing-box 侧开 `zero_rtt_handshake`)。
- keepalive / idle / MTU 周期**集中为可注入常量**(默认值逐字不变 → 零回归);为移动端自适应留参数面。

### `src/tuic.rs`
- `connect()` 首连:正常 1-RTT(无 ticket)。
- **重连路径**(`live_conn` 里的 `handshake`):改用 `connecting.into_0rtt()`;成功 → 0-RTT 发 Authenticate(或按
  退路:握手后发)+ 后续 Connect 立即 0-RTT;失败 → `.await` 1-RTT。打印 0-RTT accepted/rejected(可观测)。
- `TuicUpstream` 加 `last_udp_activity: AtomicU64`;`send_udp` 更新它;驱动任务 heartbeat 按它**按需发**。
- 抽出纯函数 `should_send_heartbeat(last_activity, now, idle_window) -> bool` 便于 TDD。

### `src/client_tun.rs`
- 基本不动(0-RTT/保活在 `TuicUpstream` 内部);若需要,把 0-RTT 开关/heartbeat 周期从 env 传入。

## 配置变化

| env | 默认 | 说明 |
|---|---|---|
| `MINI_VPN_TUIC_ZERO_RTT` | `true` | 重连是否尝试 0-RTT(排障可关,关则恒 1-RTT) |
| `MINI_VPN_TUIC_AUTH_IN_0RTT` | (e2e 定) | Authenticate 是否走 0-RTT(见头号风险;默认按 sing-box 实测) |

## 验收 recipe

### 层 1 — TDD 纯函数(CI)
- `quic.rs`:开 `enable_early_data` 后 client config 仍构建成功;keepalive 参数化后默认值不变(常量断言)。
- `tuic.rs`:`should_send_heartbeat`(活跃窗口内 true / 窗口外 false / 边界);0-RTT 开关解析。
- 0-RTT 握手本身是 I/O,不强行单测(同 13a/13b 的网络部分,由层 2 兜底)。

### 层 2 — 互通 e2e(手动,对真 sing-box,需开 `zero_rtt_handshake: true`)
1. **0-RTT 生效**:首连(1-RTT)→ 正常;kill sing-box → 客户端重连,日志显示 **0-RTT accepted**,重连后第一个
   `curl https://1.1.1.1/` 立即成功(省一次握手 RTT)。关 `MINI_VPN_TUIC_ZERO_RTT` 对照恒 1-RTT 仍正常。
2. **auth 对齐**:0-RTT 重连后 sing-box 接受 Authenticate(若拒 → 走 1-RTT-auth 退路,记录到 LEARNINGS)。
3. **保活厘清**:① idle > 30s 连接不断(QUIC keepalive);② 有活跃 UDP(`dig` 循环)时 sing-box 不回收关联;
   ③ 纯 TCP idle 时**不发** TUIC Heartbeat(抓包/日志确认 datagram 静默,仅 QUIC PING)。
4. **零回归**:`MINI_VPN_UPSTREAM=legacy` 的 TCP/UDP 全过;tuic 模式 13a(curl)+ 13b(dig)全过。

## 风险/注记

- **0-RTT keying-material**(头号,见上):early exporter 与 sing-box 是否一致 → e2e 定;不一致走 1-RTT-auth 退路。
- sing-box 必须开 `zero_rtt_handshake`,否则 `into_0rtt` 被拒、恒 1-RTT(不致命,但验收不到 0-RTT)。
- 内存 session cache:进程重启丢 ticket,首连回 1-RTT(桌面可接受;移动端持久化留后续)。
- 按需 heartbeat 的活跃阈值别太小,避免 UDP 间歇期误判为不活跃导致 sing-box 回收关联。
- 0-RTT 重放:接受代理语义下的有限风险(inner TLS 兜底)。

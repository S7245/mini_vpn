# Stage 12 — UDP relay over a QUIC datagram data plane (spec)

> grill 产出。承重决策见本文「关键设计决策」与 [ADR-0003](../adr/0003-unify-data-plane-on-quic.md)。
> 术语见 [CONTEXT.md](../../CONTEXT.md)（新增 **UDP flow** / **flow-id**）。

## 背景

到 Stage 11，mini_vpn 能透明转发任意 IP:port 的 **TCP**（fake-IP 绕 DNS 污染已通）。但 **UDP 仍是盲点**：
QUIC/HTTP3（UDP/443）与直播 UDP 完全没有数据通路，应用只能 happy-eyeballs 回退 TCP，或诡异变慢/失败。

服务端 `server.rs` 有一个 `RelayRequest::Udp` 的 SOCKS5-UDP-over-yamux 骨架，但它把 UDP 塞进**现有单条
TLS+yamux+TCP** 长连接——在「大并发、大流量、长时间、质量硬」的平台目标下，这条路对 UDP 是**结构性错误**
（HOL 阻塞、强加可靠与有序、单一拥塞控制器；对假设不可靠底层的 QUIC 尤其是语义错）。

因此本阶段**不**驱动那个骨架，而是按 ADR-0003 开第一刀：**新增一条 QUIC datagram 数据面专门承载 UDP relay**，
与现有 TCP/yamux 链路**并存、零回归**。

## 北极星与本阶段定位（ADR-0003）

- **北极星**：数据平面统一到 QUIC（`quinn`）——TCP 走可靠 QUIC stream，UDP 走 QUIC DATAGRAM(RFC 9221)。
- **第一刀（本阶段）**：只做 **UDP-over-QUIC-datagram**，新数据面独立于现有 TCP/yamux 链路。
- **后续 stage**（非本阶段）：TCP 迁 QUIC stream 退役 yamux、服务端会话表抗压、多 upstream/failover、平台化。

## 目标

1. client-tun 拦截 TUN 上的 UDP 流，经新的 QUIC datagram 连接转发到 Upstream，Upstream 代发到真实 Target 并回传。
2. 复用 fake-IP：QUIC→被墙域名（如 facebook UDP/443）在出口解析，绕本地污染。
3. UDP 流空闲自动回收；QUIC 断线自动重连，UDP 流自愈。
4. 现有 TCP relay 链路**完全不受影响**（零回归）。

## 非目标（本阶段明确不做，落 TODO future）

- TCP relay 迁移到 QUIC stream（yamux 仍承载 TCP）。
- 超限 datagram 的 stream-fallback（本阶段超限直接丢弃+计数）。
- 劫持一切 :53 / DoH / DoT 加固（只本地应答 198.18.0.1:53）。
- 服务端出口 socket 池化 / 端口耗尽抗压 / 跨用户大并发压测。
- 多 upstream / failover / 外部存储（控制面/平台 stage 的事）。

## 术语

- **UDP flow**：一条被拦截的 UDP 会话，由 app 四元组 `(srcIP:srcPort, dstIP:dstPort)` 标识。无握手、无 FIN，
  首个 datagram 诞生、空闲超时回收。每条 flow 恰好一个 **Target**。
- **flow-id**：client 铸造的 `u32`，每条 UDP flow 一个，双向随 datagram 携带，用于两端 demux。

## 架构总览（数据流）

```text
QUIC→facebook 主场景：
1. App 向系统 DNS(198.18.0.1) 查 facebook.com(A) → 拿 fake-IP 198.18.0.5（Stage 11 既有逻辑）
2. App 向 198.18.0.5:443 发 QUIC(UDP) → 198.18.0.0/15 路由进 TUN
3. 主循环 rx peek：是 UDP 且 dst≠198.18.0.1:53 → take 走 rx_buffer，走裸 relay 路径（不进 iface.poll）
4. etherparse 解出 (srcIP:sp, dst=198.18.0.5:443, payload)；resolve_target：fake → DomainPort{facebook.com,443}
5. flow 表：按四元组查/铸 flow-id；主循环造上行 datagram [flow-id][ATYP=3][facebook.com][443][payload]
6. 经 channel 交 QUIC 泵 task → conn.send_datagram(bytes)
7. server：read_datagram → 解 flow-id+target → 按 flow-id 取/建出口 UDP socket → send_to(干净解析 facebook)
8. server 收到回包 → [flow-id][payload] → send_datagram 回 client
9. client QUIC 泵 task → channel → 主循环：拆 flow-id → flow 表查回 (app端点, src=198.18.0.5:443)
   → etherparse 造 IP/UDP 包(src=fake-IP, dst=app) → push device.tx_queue → flush_tx
```

## 线格式（QUIC DATAGRAM）

精简二进制，**不背 SOCKS5 RSV/FRAG**。多字节字段大端序。

```text
上行 (client→server):
  [flow-id : u32][ATYP : u8][ADDR : 可变][PORT : u16][payload : 余下全部]
    ATYP=1  → ADDR = 4 字节 IPv4
    ATYP=3  → ADDR = [len:u8][域名 len 字节]
  （target 每包内联，服务端每包无状态解析；无建流握手）

下行 (server→client):
  [flow-id : u32][payload : 余下全部]
```

- **flow-id 由 client 铸造**（client 本就持四元组、本就维护下行映射表）。
- target 每包内联（非首包、非握手）：QUIC datagram 会丢会乱序，任何建流握手都脆；每包带 target 多 7~19 字节可忽略，
  换来零建流竞态、零半开状态（系统稳定 > 代码漂亮）。

## 模块改动

### 新增 `src/udp_relay.rs`（纯逻辑，TDD 主战场）
- `encode_uplink(flow_id, target: &TargetAddr, payload) -> Vec<u8>`：造上行 datagram 字节。
- `decode_uplink(&[u8]) -> Option<(u32, TargetAddr, &[u8])>`：服务端解上行（越界返回 None，不 panic）。
- `encode_downlink(flow_id, payload) -> Vec<u8>` / `decode_downlink(&[u8]) -> Option<(u32, &[u8])>`。
- `FlowTable`（client 用，主循环独占无锁）：
  - `intern(four_tuple) -> u32`（查/铸 flow-id）、`resolve(flow_id) -> Option<&FlowEntry>`、
    `touch(flow_id)`、`sweep(now, idle=60s)`、`MAX_UDP_FLOWS=1024` + LRU 驱逐（日志可见）。
  - `FlowEntry { app_endpoint:(IpAddr,u16), target_src:(Ipv4Addr,u16), last_activity }`。
- `build_udp_ip_packet(src, dst, payload) -> Vec<u8>`：etherparse 造下行 IPv4/UDP 包（带校验和）。
- `parse_inbound_udp(&[u8]) -> Option<UdpInbound>`：etherparse 解 (srcIP,sp,dstIP,dp,payload)。

### `src/device.rs`
- 加注入辅助 `inject_ip_packet(&mut self, pkt: Vec<u8>)`：push 进 `tx_queue`（macOS 自动补 4 字节 PI 头）。
  （现有 `flush_tx` 不变。）

### `src/client_tun.rs`
- 启动时**并行**新建 QUIC 连接（独立于 `connect_upstream`）：`connect_quic_upstream()` 建 `quinn::Endpoint`(client)
  + connect，spawn **QUIC 泵 task**（哑管道）+ 自身重连（复用 `backoff_delay` full-jitter）。
- 两条 channel：`udp_uplink`(主循环→泵 task，送整条 datagram 字节) / `udp_downlink`(泵 task→主循环，送收到的 datagram)。
- rx peek 加 UDP 分流（见「关键设计决策/分流」）。
- 主循环 `select!` 加 `udp_downlink_rx` 分支：拆 flow-id → flow 表查 → 造 IP/UDP 包 → `device.inject_ip_packet` → 主循环既有 poll/flush。
- 加 `interval(1s)` sweep 分支（或并入既有 timer 用计数守卫）回收空闲 flow。
- `resolve_target` 复用；fake 无映射 → 丢该 datagram + 日志（UDP 无 socket 可 rearm）。

### `src/server.rs`
- 新增 QUIC endpoint（`quinn::Endpoint` server，监听 **UDP**，默认复用现有 bind 端口号；证书/私钥复用现有加载代码 + ALPN）。
- 每条 QUIC 连接 spawn 一个 task：`read_datagram` 循环 → `decode_uplink` → 按 flow-id 取/建**出口 UDP socket**
  （`flow-id → UdpSocket` 会话表，每 flow 一个 ephemeral 端口）→ `send_to(target)`；
  每个出口 socket 一个 recv task：回包 → `encode_downlink` → `send_datagram` 回 client。
- 会话表空闲 60s 回收（关 socket）。**现有 TCP/yamux accept 循环与 `RelayRequest::Udp` 骨架不动**（骨架本阶段不再使用）。

### `Cargo.toml`
- 加 `quinn = "0.10"`（共享 rustls 0.21；etherparse/bytes/rustls 已在）。

## 关键设计决策（grill 产出）

1. **UDP 绕过 smoltcp 走裸包**：smoltcp 0.10 `UdpMetadata` 无 `local_address`，拿不回目的 IP；且非接口 src 的 UDP
   egress 踩 DNS 那个坑。裸包（etherparse 解析+造包）直接消灭这一整类风险。smoltcp 只留给 TCP 与 DNS resolver。
2. **flow 表主循环独占、QUIC 泵 task 当哑管道**：跨 task 只传字节，零共享状态；所有 encode/decode + flow 表是主循环里
   的纯函数（可单测）。下行 IP 包在主循环造。
3. **QUIC 重连不复位 UDP 流**：UDP 无连接状态，下个 datagram 让服务端重建出口 socket 自愈（长直播流不被打断）。
4. **空闲回收**：双侧各 60s 独立超时、自愈兜底；client 1s sweep；`MAX_UDP_FLOWS=1024` + LRU（日志可见）。常量，本阶段不开 env。
5. **超限 datagram**：内层+头 > `max_datagram_size()` → **丢弃+计数+日志**（不静默）。外层 QUIC `max_datagram_frame_size`
   配足以保证 1200 的 QUIC initial+头能过（HTTP3 握手命门）。stream-fallback 留 TODO。
6. **rx 分流（D1）**：`UDP && dst==198.18.0.1:53` → smoltcp(DNS 原样)；其它 UDP → 裸 relay；非 UDP → 现有路径。
7. **端口复用 + 启动即全或无**：QUIC 监听 UDP 复用现有端口号（用户在 App 零额外配置即用 TCP/UDP）；QUIC listener
   起不来则**整体启动失败**（不跑半套）。
8. **依赖底座**：`quinn 0.10.2` 依赖 `rustls ^0.21` → 共享现有 rustls 0.21.12，证书加载代码原样复用；ALPN 经 rustls-config
   路径设 `b"mvpn"`，用 `quinn::ServerConfig::with_crypto` / `quinn::ClientConfig::new` 包装。

## 配置变化

| 项 | 说明 |
|---|---|
| `MINI_VPN_SERVER_QUIC_BIND_ADDR` | 可选覆盖；默认从现有 `MINI_VPN_SERVER_BIND_ADDR` 派生同端口号(UDP) |
| `MINI_VPN_TUN_SERVER_ADDR` | 复用；QUIC upstream 默认取同 host:port(UDP) |
| `MINI_VPN_TUN_CA_PATH` / `MINI_VPN_TUN_TLS_SNI` | 复用（QUIC 用同 CA 校验、同 SNI） |

编译期常量：`UDP_FLOW_IDLE_SECS=60`、`MAX_UDP_FLOWS=1024`、`UDP_SWEEP_INTERVAL=1s`、ALPN=`b"mvpn"`。

## 验收 recipe（三层）

### 层 1 — TDD 纯函数（CI，无 sudo）
`udp_relay.rs` 单测：上/下行帧 encode↔decode round-trip（含 ATYP=1/3、越界返回 None）；`FlowTable` intern/resolve/
touch/sweep/LRU 驱逐；`build_udp_ip_packet`↔`parse_inbound_udp` round-trip + 校验和；超限丢弃+计数；
`resolve_target` 在 UDP 上 fake→域名 / 非 fake→IPv4 / fake 无映射→丢。

### 层 2 — 本地确定性集成（CI，无 TUN）
client QUIC ↔ server QUIC 走 localhost，**合成 flow 喂 datagram**（绕 TUN），server relay 到本地 UDP echo server，
验证双向 round-trip + flow-id demux + 空闲回收。覆盖 quinn 传输 + 服务端会话表 + 帧。

### 层 3 — 手动跨机 e2e（sudo/TUN）
US Upstream + 深圳 client（沿用 Stage 9-11 起法；**server 真实 IP 不可路由进 TUN**，只放 `198.18.0.0/15` + 测试目标）。

```bash
UT=$(ifconfig | awk '/^utun/{i=$1} /inet 10\.0\.0\.1 /{print i}' | tr -d ':')
sudo route -n add -net 198.18.0.0/15 -interface "$UT"
networksetup -setdnsservers Wi-Fi 198.18.0.1            # 用完务必恢复
```

- **UDP echo（先，证 datagram 链路）**：路由某 UDP echo 目标进 TUN，`nc -u <target> <port>` 打过去收到回显；
  client 日志 `🌊 udp flow ... flow-id=N`、server 日志 `📨 udp relay ... → <target>`。
- **QUIC/HTTP3 主场景**：用 **Reqable 的 REST/API 客户端**（非抓包代理；关其内置 DoH）compose `GET https://www.facebook.com/`
  **强制 HTTP/3** → 拿到真实 Meta 响应（协议栏显示 h3）；client 日志显示 flow-id + 域名 target，server 日志
  `解析出的目标地址是: www.facebook.com:443` 并建出口 socket。
- **质量冒烟**：Reqable 持续 HTTP/3 下载 ~1–2 分钟 + 几条并发 QUIC 流，肉眼确认不卡顿、无内存/端口泄漏。
- **不回归**：现有 TCP 场景（`curl http://1.1.1.1/`、`curl https://1.1.1.1/`）仍正常。

```bash
networksetup -setdnsservers Wi-Fi empty                # 恢复 DNS（重要）
sudo route -n delete -net 198.18.0.0/15
```

预期同时验证：QUIC datagram 数据面通 ✅、fake-IP 在 UDP 上复用（出口解析绕污染）✅、UDP 流回收/自愈 ✅、TCP 零回归 ✅。

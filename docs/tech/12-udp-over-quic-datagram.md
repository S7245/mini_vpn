# 12 UDP over QUIC datagram（第一刀）

## 背景

到 Stage 11，mini_vpn 能透明转发任意 IP:port 的 **TCP**（fake-IP 绕 DNS 污染已通），但 **UDP 是盲点**：
QUIC/HTTP3（UDP/443）、直播 UDP 无数据通路。Stage 12 按 [ADR-0003](../adr/0003-unify-data-plane-on-quic.md)
开第一刀：**新增一条 QUIC datagram 数据面专门承载 UDP relay**，与现有 TCP/yamux 链路并存、零回归。

中文要点：北极星是「数据平面统一到 QUIC」（TCP 走 QUIC stream、UDP 走 QUIC datagram）。本阶段只做
UDP-over-QUIC-datagram；TCP 迁移、平台化是后续 stage。**为什么不把 UDP 塞进现有 TCP 隧道**：UDP-over-TCP
强加可靠+有序+HOL 阻塞，对 QUIC 是语义错、对直播是质量错——在「大并发大流量长时间、质量硬」下结构性不可行。

## 为什么 UDP 绕过 smoltcp 走裸包

smoltcp 0.10 的 `UdpMetadata` 只有 `endpoint`（远端/源），**没有 `local_address`**——一个 `addr:None`
的 UDP 监听 socket 收到包后拿不回目的 IP（target 丢了）；且非接口 src 的 UDP 回程 egress 还要踩 Stage 11
DNS 那个坑。UDP 没有握手/重传/窗口，smoltcp 状态机对它没增值。

所以 UDP **完全绕过 smoltcp**：用 etherparse 解入站裸 IP/UDP（拿到 src/dst/payload），下行用 etherparse 造
IP/UDP 包直接注入 `device.tx_queue`。这把整类 `local_address`/egress-src 风险**直接删除**。smoltcp 只留给
TCP 和 fake-IP DNS resolver。（系统稳定 > 代码漂亮：接受 TCP 走 smoltcp / UDP 裸包的双心智模型。）

## 全链路数据流（QUIC→facebook 主场景）

```text
1. App 查 facebook.com(A) 到 198.18.0.1 → 拿 fake-IP 198.18.0.5（Stage 11 既有）
2. App 向 198.18.0.5:443 发 QUIC(UDP) → 198.18.0.0/15 路由进 TUN
3. rx peek 分流：UDP 且 dst≠198.18.0.1:53 → take 走 rx_buffer，裸 relay（不进 iface.poll）
4. parse_inbound_udp → (src, dst=198.18.0.5:443, payload)；resolve_target：fake → DomainPort{facebook.com,443}
5. FlowTable.intern(四元组) → flow-id；encode_uplink [flow-id][ATYP=3][facebook.com][443][payload]
6. → udp_uplink channel → QUIC 泵 task → conn.send_datagram
7. server read_datagram → decode_uplink → 首包解析域名一次 + connect 出口 socket → send(payload)
8. 出口 socket 收到回包 → encode_downlink [flow-id][payload] → send_datagram 回 client
9. client 泵 → udp_downlink channel → 主循环 decode_downlink → FlowTable.resolve(flow-id)
   → build_udp_ip_packet(src=fake-IP 198.18.0.5:443, dst=app) → device.inject_ip_packet → flush_tx
```

## 线格式（QUIC DATAGRAM，精简二进制，大端）

```text
上行 client→server:  [flow-id:u32][ATYP:u8][ADDR][PORT:u16][payload]   ATYP 1=IPv4 / 3=[len]domain / 4=IPv6
下行 server→client:  [flow-id:u32][payload]
```

- **flow-id 由 client 铸造**（每四元组一个），双向带，解开下行 demux 死结：服务端回程带的是**真实 target IP**，
  client 只有 `domain→fake-IP` 映射、反查不回，所以必须有显式 flow-id。
- **target 每包内联、无握手**：QUIC datagram 会丢会乱序，任何建流握手都脆；每包带 target → 服务端逐包无状态，
  零建流竞态、零半开。

## 模块

- `src/udp_relay.rs`（lib，纯逻辑收口 + 服务端 relay）：帧编解码、`FlowTable`（四元组↔flow-id、60s 空闲 sweep、
  `MAX_UDP_FLOWS=1024` LRU）、`build_udp_ip_packet`/`parse_inbound_udp`（etherparse）、`serve_quic_connection`
  （flow-id→出口 socket 会话表）、`expired_flow_ids`。
- `src/quic.rs`（lib）：QUIC server/client config（复用 rustls 0.21 证书 + ALPN `mvpn`）+ endpoint 构建。
- `src/device.rs`：`inject_ip_packet`（裸包加帧入 tx_queue，macOS 补 4 字节 PI 头）。
- `src/server.rs`：QUIC endpoint accept loop（与 TCP listener 并存，复用同端口号 UDP；起不来即启动失败）。
- `src/client_tun.rs`：QUIC 泵 task（哑管道 + full-jitter 重连）+ 双 channel + rx 分流 + 下行注入 + 1s sweep。

## 关键设计取舍（grill 产出）

- **flow 表主循环独占、QUIC 泵当哑管道**：跨 task 只传字节，下行 IP 包在主循环造，单一事实源、好测。
- **QUIC 重连不复位 UDP 流**：无连接状态，下个 datagram 让服务端重建出口 socket 自愈（长直播不被打断）。
- **服务端首包解析一次 + connect 出口 socket**（code-review 加固）：解析移出每包热路径（避免 HOL + 重复 DNS，
  契合「解析属控制面、不进数据面热路径」）；按 target 地址族绑定（IPv6 域名也能走）；connect 后只收该对端的回包
  （杜绝 off-path UDP 伪造，对 DNS-over-UDP 尤其重要）。
- **超限 datagram 丢弃 + 计数日志**（两端都不静默）；stream-fallback 留 TODO。
- **rx 分流 D1**：只本地应答 198.18.0.1:53，其它 :53 与一切 UDP 走 relay。

## 依赖底座

`quinn 0.10.2` 依赖 `rustls ^0.21` → **共享项目现有 rustls 0.21.12**，证书加载代码原样复用，**不引入第二个
rustls**。datagram API：`send_datagram(Bytes)` / `read_datagram()` / `max_datagram_size()`。

## 验收 recipe

### 本地确定性（CI，无 TUN）
`cargo test`：`udp_relay` 纯函数单测 + `tests/udp_quic_relay.rs`（client QUIC↔server QUIC over loopback →
本地 UDP echo，双向 round-trip + flow-id demux）。

> ⚠️ 本沙箱里 `cargo test --doc` 会被 SIGKILL（rustdoc 链接大 rlib 触发资源上限），**非代码问题**——
> lib 无任何 doctest。用 `cargo test --lib --bins --tests` 看真实结果。

### 跨机 e2e（sudo/TUN，需手动）
US Upstream + 深圳 client（沿用 Stage 9–11 起法；**server 真实 IP 不可路由进 TUN**，只放 `198.18.0.0/15` +
测试目标，否则 QUIC 出网回环）。

```bash
UT=$(ifconfig | awk '/^utun/{i=$1} /inet 10\.0\.0\.1 /{print i}' | tr -d ':')
sudo route -n add -net 198.18.0.0/15 -interface "$UT"
networksetup -setdnsservers Wi-Fi 198.18.0.1            # 用完务必恢复
```

- **UDP echo（先，证 datagram 链路）**：路由某 UDP echo 目标进 TUN，`nc -u <target> <port>` 收到回显；
  client 日志 `🌊 ... flow`、server 日志 `📨 ...`。
- **QUIC/HTTP3 主场景**：**Reqable 的 REST/API 客户端**（非抓包代理；关其内置 DoH）compose
  `GET https://www.facebook.com/` **强制 HTTP/3** → 真实 Meta 响应（协议=h3）；client 日志 flow-id + 域名 target、
  server 日志 `解析出的目标地址是: www.facebook.com:443`。
- **质量冒烟**：Reqable HTTP/3 持续下载 ~1–2 分钟 + 几条并发流，肉眼无卡顿/泄漏。
- **不回归**：`curl http://1.1.1.1/`、`curl https://1.1.1.1/` 仍正常。

```bash
networksetup -setdnsservers Wi-Fi empty                # 恢复 DNS（重要）
sudo route -n delete -net 198.18.0.0/15
```

## 验收结果（2026-06-08，跨机签收）

深圳 client（macOS）↔ US exit（弗吉尼亚 VPS），全部通过：

| 项 | 结果 |
|---|---|
| ATYP=1（IP 直连） | `dig @1.1.1.1` 拿到真实应答；IP echo 回显 ✅ |
| ATYP=3（fake-IP→域名） | `udp.zkwcloud.com` echo 回显，server `📨 ... → 47.252.50.10:9999` ✅ |
| 1200B 冷启动（≈QUIC initial） | `got 1200`，刚连上即可（initial_mtu=1280）✅ |
| 连接长稳 | keepalive=5s，`✅ 已连接` 常驻不闪 ✅ |
| 持续 60s | 48/48（100%）✅ |
| 并发 160 流（无 DNS / 有 DNS） | **160/160 / 160/160** ✅ |
| TCP 不回归 | `curl https://1.1.1.1/` 正常 ✅ |

> 跨机 bring-up 期间修复（均已合入）：keepalive + idle（be133c5/841f82a）、超限 datagram 改 connect 出口 socket
> （f903a58）、热路径逐包日志拖垮主循环（185cc42）、initial_mtu=1280 消除冷窗口（9252e75）、fake DNS resolver
> buffer 8→64 槽（cec9961）。**踩坑细节见 .learnings/LEARNINGS.md（2026-06-08）。**
> 测试注意：echo 用单 socket 的 Python（`recvfrom`/`sendto`），别用 `ncat -k -u -e`（每对端 fork、扛不住并发）。

## MTU / datagram 尺寸（实测行为）

外层 QUIC datagram 可用大小 = `conn.max_datagram_size()`，随路径 MTU 变化。quinn 从安全的 initial MTU 1200
起步（此时 ~1162），**PLPMTUD 探测后升到 ~1414**（1500 MTU 路径；loopback 实测 1162→1414）。

- 含义：内层 1200B UDP 包（典型 QUIC initial）+ 我们 ~20B 头 ≈ 1224 —— **冷连接(1162)装不下、warm 后(1414)装得下**。
- 数据面是**持久连接**（启动即连 + keep-alive=5s 常驻），用户发请求时早已 warm → 真·QUIC 的 1200 initial 能过。
- 冷窗口（刚 (重)连那一瞬）发超大包会被丢（记日志）；QUIC 会重传 initial、warm 后自愈。
- 回归测试 `udp_relay_carries_full_1200_byte_payload` 钉住「warm 后 1200B 不截断」。

## 已知限制（→ TODO future）

- 超限 datagram 丢弃（无 stream-fallback）；超过 warm 后 `max_datagram_size`(~1400) 的大 UDP 包/超大 DNS 响应会丢，
  QUIC-inside 靠内层 PMTUD 缩包自愈。
- 服务端会话表朴素「每流一 socket」，无端口耗尽/池化抗压。
- 只本地应答 198.18.0.1:53；DoH/DoT、硬编码 DNS 不拦。
- TCP relay 仍在 yamux（未迁 QUIC）；多 upstream/failover、外部存储是平台 stage。

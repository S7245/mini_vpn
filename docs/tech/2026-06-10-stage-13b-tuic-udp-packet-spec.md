# Stage 13b — UDP relay via TUIC Packet (native datagram) (spec)

> grill 产出。第二刀:在 `tuic` 模式下,把拦截到的 **UDP** 经 **TUIC `Packet`(native datagram)** 转发到
> sing-box。复用 Stage 12 的裸包 UDP 拦截 + 下行注入,只是把自定义帧换成 TUIC Packet,发到 13a 那条 TUIC
> 连接的 datagram 上。完成后 tuic 模式即 TCP+UDP 全量,可替代 legacy。见 ADR-0004 / 13a。

## 目标

1. `tuic` 模式下:拦截 UDP → 编 TUIC `Packet` → 经 TUIC 连接 `send_datagram` 到 sing-box;下行 datagram →
   解 TUIC `Packet` → 造回程 IP/UDP 包注入 TUN。
2. **每 4 元组一个 u16 `ASSOC_ID`**(≈ Stage 12 flow-id),换干净下行 demux(死结解法同 Stage 12)。
3. **fake-IP→域名**复用(ATYP=域名);非 fake → IPv4。
4. 周期发 **Heartbeat**(datagram)保活 TUIC UDP 会话(spec 合规 + 服务端不回收)。
5. legacy 模式 UDP 路径(Stage 12)**零改动**。

## 非目标(→ 后续)

- **quic-stream 模式 / 分片 / 超大包**(>datagram 上限)→ 13b 下半场或单列(本刀只 native datagram;
  超限丢弃+计数,沿用 Stage 12 策略;initial_mtu=1280 已保证 QUIC initial 装得下)。
- 迁移/0-RTT 调优 → 13c;退役 legacy → 13d。

## 术语(见 CONTEXT.md)

- **assoc-id**:TUIC UDP 关联 id(u16)。本项目**每条 UDP flow(4 元组)分配一个**,与 [[flow-id]] 同义,只是
  上线宽度 16 位、用于 TUIC `Packet` 帧。
  _Avoid_: session id(它标识一条 UDP flow,不是 QUIC stream/TCP session)

## TUIC `Packet` 线格式(native 模式走 datagram)

```text
[VER=0x05][TYPE=0x02][ASSOC_ID:u16][PKT_ID:u16][FRAG_TOTAL:u8][FRAG_ID:u8][SIZE:u16][ADDR][data]
```
- native 不分片:`FRAG_TOTAL=1`、`FRAG_ID=0`、`PKT_ID` 任意(本刀固定 0 或递增计数均可,native 不重组)。
- `SIZE` = `data` 字节数。`ADDR` = TUIC 地址(ATYP 0x00 域名/0x01 IPv4/0x02 IPv6,见 13a)。
- **Heartbeat**:`[0x05][0x04]`(空,datagram)。

## 架构(镜像 Stage 12)

```text
上行(tuic 模式):
  rx 分流 UdpRelay → parse_inbound_udp → resolve_target(fake→域名)
  → AssocTable.intern(4元组)=assoc_id → encode_tuic_packet(assoc_id, target, data)
  → TuicUpstream.send_udp(bytes)  [conn.send_datagram]
下行:
  TuicUpstream 的 datagram 泵: conn.read_datagram() → decode_tuic_packet → (assoc_id, data)
  → channel → 主循环: AssocTable.resolve(assoc_id)=(app端点, fake-IP源)
  → build_udp_ip_packet(src=fake-IP, dst=app) → device.inject_ip_packet → flush
保活: 周期(如每 3s,有活跃 UDP 时)send_datagram(Heartbeat)
```
- **AssocTable 主循环独占**(同 Stage 12 flow 表),无锁;`now` 注入便于单测。
- **TuicUpstream 当 datagram 哑管道**(send_udp / 下行泵),帧 encode/decode + assoc 表都在主循环(纯函数,可测)。

## 模块改动

### `src/tuic.rs`
- `encode_packet(assoc_id, target, data) -> Vec<u8>` / `decode_packet(&[u8]) -> Option<(u16, &[u8])>`
  （下行只需 assoc_id + data;上行 target 内联）—— **纯函数 TDD**。
- `encode_heartbeat() -> Vec<u8>`。
- `TuicUpstream`:加 `send_udp(&self, datagram: Vec<u8>)`(conn.send_datagram,满/超限丢弃+计数)+
  下行 datagram 泵 task(读 datagram → 经 channel 给主循环)+ 周期 heartbeat。
- `AssocTable`(u16):intern(4元组)→assoc_id、resolve、touch、sweep、LRU(≈ FlowTable,id u16)。
  （可把 FlowTable 泛化为 id 类型,或单写一份;取简单可靠者。)

### `src/client_tun.rs`
- tuic 模式启用 UDP 路径:rx 分流 UdpRelay → 走 TUIC 上行(而非 Stage 12 的 run_quic_pump);
  新增/复用 udp_downlink 分支:tuic 模式下数据来自 TuicUpstream 的下行泵。
- legacy 模式分支**逐字不变**。

## 配置变化

| env | 默认 | 说明 |
|---|---|---|
| `MINI_VPN_TUIC_UDP_MODE` | `native` | 本刀只实现 native;quic-stream 留后续 |

## 验收 recipe

### 层 1 — TDD 纯函数(CI)
`encode_packet`/`decode_packet`(各 ATYP、round-trip、越界 None)、`encode_heartbeat`、`AssocTable`
(intern 稳定/唯一、resolve、sweep、LRU、u16 上限)。

### 层 2 — 互通 e2e(手动,对真 sing-box)
sing-box(13a 那台,已支持 TUIC UDP)。客户端 `MINI_VPN_UPSTREAM=tuic`(同 13a 参数):
- **DNS over UDP**:`dig @1.1.1.1 example.com`(路由 1.1.1.1 进 TUN)→ 拿到应答(经 sing-box 出口)。
- **UDP echo(域名,ATYP=域名)**:路由 fake 段 + DNS→198.18.0.1,Python 单 socket echo 在 VPS,
  `nc/python` 打 `udp.zkwcloud.com:9999` → 回显;client 日志 assoc、sing-box 日志 UDP relay。
- **QUIC/HTTP3**(可选):Reqable/curl-http3 → facebook,经 sing-box 出 h3。
- 不回归:`MINI_VPN_UPSTREAM=legacy` 的 Stage 12 UDP 仍正常。

## 风险/注记
- assoc_id u16:`MAX_UDP_FLOWS` 仍 1024,远低于 65536,无压力;分配器单调+LRU 防回绕冲突。
- 超限 datagram:native 不分片,>max_datagram_size 丢弃+计数(quic-stream 回退留后续)。
- heartbeat 频率别太密(电量;移动端 13c 再自适应)。

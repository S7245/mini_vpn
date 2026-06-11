# 13b TUIC client — UDP relay via Packet (native datagram) — setup + acceptance

> Stage 13b:client-tun 把拦截到的 **UDP** 经 **TUIC `Packet`(native QUIC datagram)** 转发到成熟
> **sing-box TUIC 服务端**,与 13a 的 TCP **复用同一条已认证 QUIC 连接**。双轨
> (`MINI_VPN_UPSTREAM=legacy|tuic`,默认 legacy 零回归)。设计见 ADR-0004 + 13b spec/plan。
> ⚠️ **凭据(UUID/password)不要提交进 git**;本文用占位符。

## 验收结果(2026-06-11,跨机签收)

深圳 client(tuic 模式)→ **sing-box TUIC server**(UDP 8443)→ 出网,三项通过:

- `dig @1.1.1.1 example.com` → `NOERROR`,A 记录 `172.66.147.243 / 104.20.23.154`(Cloudflare 真 IP)。
  `@1.1.1.1` **不命中**本地 fake-resolver(198.18.0.1:53),所以这是一条真正的 **UDP DNS 经 TUIC
  `Packet` → sing-box → 1.1.1.1:53 出网并回程**,而非本地伪造应答。
- `dig @1.1.1.1 facebook.com` → A 记录 `157.240.211.35`(Meta 真 IP):第二条 UDP flow 复用 `AssocTable`
  按 assoc-id demux,下行注入正确。
- `curl https://1.1.1.1/` → `HTTP/2 301` + Cloudflare 真证书(CN=cloudflare-dns.com),`cf-ray …-HKG`:
  **TCP(13a)零回归** —— TCP 双向流与 UDP datagram 跑在同一条 QUIC 连接上互不干扰。

> 意义:UDP/DNS(以及 QUIC/HTTP3)正式经 sing-box 出网,mini_vpn 的 UDP 数据面与成熟 TUIC 生态互通。
> 下一刀 13c:connection migration + 0-RTT + 自适应 heartbeat(移动漫游/弱网)。

## TUIC `Packet` 线格式(native datagram)

```
[VER=0x05][TYPE=0x02][ASSOC_ID:u16][PKT_ID:u16][FRAG_TOTAL:u8][FRAG_ID:u8][SIZE:u16][ADDR][data]
```
- native(无分片):`FRAG_TOTAL=1`、`FRAG_ID=0`、`PKT_ID=0`、`SIZE=data.len()`。
- `ADDR` = TUIC 地址 `[ATYP][ADDR][PORT:u16 BE]`,ATYP **0x00=域名`[len][bytes]` / 0x01=IPv4 / 0x02=IPv6**
  (与 13a Connect 同一套地址编码;注意 ATYP 取值与 Stage-12 自定义码不同)。
- **Heartbeat**:`[0x05][0x04]`,空闲时周期发(本实现 3s),维持 NAT 映射/路径活性。
- 下行解码只取 `(assoc_id, data)`:按 ATYP 跳过 `ADDR`,越界/未知 ATYP 一律返回 `None`(绝不 panic)。

> 编码器有字节级单元测试,但**真理是"sing-box 收下并回程了"**:`dig` 拿到真实应答即证明 Packet/地址布局
> 与 sing-box 字节对齐。

## 数据面接线(`src/tuic.rs` + `src/client_tun.rs`)

- **一条连接、单一事实源**:`TuicUpstream::live_conn()` 取活连接(断了就地重连+重认证,13a 逻辑),
  TCP(`open_tcp`)与 UDP(`send_udp` / 驱动任务)共用,由 `conn` 互斥锁串行化重连——绝不产生第二条连接。
- **上行(主循环,`AssocTable` 主循环独占)**:rx 分类为 `UdpRelay` → `parse_inbound_udp` →
  `resolve_target`(fake-IP→域名 ATYP=0x00 / 直 IP ATYP=0x01)→ `AssocTable.intern`(每 4 元组一个 u16
  assoc-id)→ `encode_packet` → `TuicUpstream::send_udp`(`conn.send_datagram`;TooLarge/连接错 → 丢弃+计数,
  UDP 语义)。
- **下行(后台驱动任务 + 主循环 select)**:`start_udp()` spawn 一个**自愈驱动任务**——每代连接 `select`
  `read_datagram`(→ 下行 channel,`send().await` 施加背压**不丢 DNS 响应**)与 heartbeat tick;连接断 → 退避
  → `live_conn` 重连,泵与心跳自然恢复。主循环 select 下行 channel → `decode_packet` → `AssocTable.resolve`
  → `build_udp_ip_packet(src=target, dst=app)` → `device.inject_ip_packet`。
- **回收**:`udp_sweep`(1s)同时扫 legacy `FlowTable` 与 tuic `AssocTable`,回收空闲 60s 的 flow/assoc。

> 形态刻意与 Stage-12 裸包 UDP(`handle_udp_uplink` / `run_quic_pump`)同构,只把 flow-id/自研 codec 换成
> assoc-id/TUIC codec;**legacy 路径字节不变(零回归)**,tuic 模式下其占位 channel 让 legacy select 分支永久休眠。

## 客户端参数(tuic 模式;凭据从环境/文件注入,勿入库)

与 13a 完全相同(见 `docs/tech/13-tuic-tcp-connect.md` 的参数表):`MINI_VPN_UPSTREAM=tuic` +
`MINI_VPN_TUIC_SERVER/UUID/PASSWORD/SNI/CA_PATH/ALPN`。sing-box server 配置见 13a(同一个 inbound 同时承载
TCP Connect 与 UDP Packet,无需额外配置)。

## 验收 recipe

```bash
sudo MINI_VPN_UPSTREAM=tuic \
  MINI_VPN_TUIC_SERVER=<VPS_IP>:8443 \
  MINI_VPN_TUIC_UUID=<UUID> MINI_VPN_TUIC_PASSWORD=<PASS> \
  MINI_VPN_TUIC_SNI=example.com MINI_VPN_TUIC_CA_PATH=certs/dev/ca-cert.pem MINI_VPN_TUIC_ALPN=h3 \
  ./target/debug/mini_vpn client-tun

# UDP DNS(经 TUIC Packet,@1.1.1.1 不命中本地 fake-resolver):
dig @1.1.1.1 example.com      # 期望:NOERROR + 真实 A 记录
dig @1.1.1.1 facebook.com     # 期望:第二条 flow 也回程(AssocTable demux)
# TCP 非回归(13a):
curl -v -k -m 15 https://1.1.1.1/   # 期望:HTTP/2 301 + Cloudflare 真证书
```
**期望**:`dig` 拿到真实应答;sing-box 日志显示来自该 UUID 的 **UDP/Packet** assoc;idle 时 heartbeat 维持连接。
切回 `MINI_VPN_UPSTREAM=legacy`,UDP 仍正常(零回归)。

> 拓扑约束沿用既有:server/sing-box 的真实 IP 不可路由进 TUN(防回环)。`@1.1.1.1` 走 UDP relay 是因为
> 本地只对 198.18.0.1:53 做 fake-IP 应答,其它 :53 一律进 relay(见 stage-12 spec 的 D1 分类规则)。

## 关键坑 / 设计决策

- **assoc-id 是 u16,空间会回绕**。`AssocTable` 沿用 `FlowTable` 的「单调递增、绝不复用」机制,但 u16 仅 65535:
  `next_id` 回绕后可能落到**仍在册**的 id 上,`intern` 必须跳过它,否则覆盖活跃 flow(回程串到错误端点)并泄漏
  `tuple_to_id`。活跃集(≤1024)远小于 id 空间,扫描代价极小。legacy `FlowTable`(u32)回绕不可达,保持不动。
- **下行用 `send().await` 背压、不丢**:DNS 响应丢了会导致 `getaddrinfo` 失败;上行用丢弃计数(UDP 语义)。
- **single connection 复用**:UDP datagram 必须走**已认证**的那条连接(sing-box 校验 auth),所以与 TCP 共用一条,
  不能另开匿名连接。

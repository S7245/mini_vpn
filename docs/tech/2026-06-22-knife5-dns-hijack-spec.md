# 刀5 — 拦全 :53 裸包 DNS 劫持 spec

> 配套：plan(同目录 `2026-06-22-knife5-dns-hijack-plan.md`)、ADR `docs/adr/0007-hijack-all-plaintext-dns.md`、
> findings(复用并续写 `2026-06-12-knife1-bottleneck-findings.md` 末节)。分支 `claude/knife5-dns-hijack`(从 main 起)。
> 对症「无缝 on/off、不依赖系统 DNS」:刀4 拦加密 DNS 逼回落明文,但回落到的是 app **自己配的 resolver**
> (如 `8.8.8.8:53`),不是 198.18.0.1。现状只伪造发往 198.18.0.1:53 的查询 → 其它 resolver 的明文查询被隧道
> 转发到真 DNS → app 拿**真实 IP** → 绕过 fake-IP。北极星:**任意 resolver 的明文 :53 都本地伪造 fake-IP**。

## TL;DR

| 项 | 缺口 | 本刀做法 |
|---|---|---|
| **拦全明文 :53** | `classify_inbound` 仅 `dst==198.18.0.1:53` 走本地伪造;其它 :53 → 隧道转发真 DNS → 真实 IP 绕过 fake-IP | `classify_inbound` 任意 `:53`(任意 dst IP)→ `Dns`;**裸包**伪造回包(`src=被查询的 resolver`),不进 smoltcp |
| **源地址正确** | smoltcp socket 只能以本接口 IP 当 src,无法为无界 resolver IP 集合回包 | 复用 `build_udp_ip_packet(src=(原dst,53), dst=(原src,原srcport))` + `inject_ip_packet`(UDP relay 下行同款,已验收) |
| **废 smoltcp DNS socket** | 198.18.0.1 socket + 接口 IP `198.18.0.1/32` + `drain_dns` 是特例第二路径 | 删除三者,**统一一条裸包伪造路径**(含 198.18.0.1) |
| **TCP :53 泄漏** | 只拦 UDP :53,TCP DNS 仍隧道转发真 DNS → 真实 IP 泄漏 | `resolve_target` 加 `port==53 → Block` → **RST**(复用刀4 TCP Block 接线);只命中 TCP(UDP :53 已被 classify 截走) |

## 现状(代码事实,已查证)

- **`classify_inbound`**([client_tun.rs:1177](../../src/client_tun.rs)):`dst_ip==198.18.0.1 && dst_port==53` → `Dns`(smoltcp 本地伪造);其它 UDP → `UdpRelay`;非 UDP → `Other`。**`8.8.8.8:53` 当前 → `UdpRelay`**(测 [:1532](../../src/client_tun.rs) 坐实)。
- **smoltcp DNS socket**:`dns_handle` 绑 `198.18.0.1:53`([:439](../../src/client_tun.rs));接口配 `198.18.0.1/32`([:460](../../src/client_tun.rs))**仅**为让 smoltcp 能以 `src=198.18.0.1` 回包(否则按子网选源、回包 src 对不上、被系统丢弃)。`drain_dns`([:854](../../src/client_tun.rs))在每次 poll/timer 后处理查询。
- **裸包下行注入(要复用的模式)**:UDP relay 下行 `build_udp_ip_packet(src,dst,payload)` + `device.inject_ip_packet()` + `flush_tx()`([:607](../../src/client_tun.rs))。`build_udp_ip_packet`([udp_relay.rs:73](../../src/udp_relay.rs))的 src 可为**任意 IP**。
- **DNS 伪造逻辑(要复用)**:`dns::parse_query`/`build_response`/`Answer`([dns.rs](../../src/dns.rs))已就绪——单 question、A→fake-IP、AAAA/其它→NODATA、不可解析→None。
- **`resolve_target`**([:810](../../src/client_tun.rs))= TCP 首包 + UDP 包**共享**决策点;刀4 已有 `Block`,TCP→`rearm_socket`(RST,[:976](../../src/client_tun.rs))、UDP→drop([:1217](../../src/client_tun.rs))。
- **fake-IP 池**:`.1` 已预留不分配([fake_ip.rs:36](../../src/fake_ip.rs)),`alloc` 稳定复用、引用计数回收(刀2)。

## 设计决策(grill 对齐,2026-06-22,见 ADR-0007)

- **D1 裸包 vs AnyIP → 裸包**。AnyIP 要让 smoltcp 以 `src=被查询的 resolver` 回包,须把每个 resolver IP 加成接口 IP——resolver 是**无界集合**(8.8.8.8/1.1.1.1/企业内网…),死路。裸包复用已验收的 UDP relay 下行注入,天生支持任意 src。
- **D2 废 smoltcp DNS socket,统一裸包(含 198.18.0.1)**。删 `dns_handle`/`bind`/接口 IP `198.18.0.1/32`/`drain_dns` 两处调用。**一条 DNS 伪造路径**(DRY,杜绝两路发散);198.18.0.1 降格为「对外广告的 resolver 地址」(前端 NE 配),数据面不再特殊待它。
- **D3 源地址(正确性钉死)**:回包 `src=(原查询 dst,53)`、`dst=(原 src_ip,src_port)`。`:53` 劫持在 `resolve_target` **之前、无条件**——哪怕 dst 落 fake-IP 段也照样伪造(`:53` 路径不调 resolve_target)。
- **D4 劫持范围 → 全劫持,不按 dst 过滤**。TUN 上任意 `:53`(含 RFC1918/LAN)都伪造。最简最稳、无 DNS 逃逸;split-horizon/纯内网域名记为已知限制(全隧道下 on-link LAN DNS 走物理网卡、不进 TUN,实际罕见)。后续真需要再加 allowlist。
- **D5 TCP :53 → RST**。`resolve_target` 加 `port==53 → Block`;**不变量**:UDP :53 已被 `classify_inbound` 截走、不到 resolve_target → `port==53` 只命中 **TCP**。复用刀4 TCP `Block→rearm(RST)`,逼应用回落 UDP :53(我方应答极小、永不触发 TC 截断 → 不升级 TCP)。
- **D6 与刀4 互补(纯叠加、无冲突)**:`:53`→Dns(伪造)、`:853`→Block(刀4)、`:443` DoH→Block(刀4),端口分流互不干扰。刀4 逼回落明文,刀5 让任意 resolver 的明文回落闭环——**刀5 不改刀4 一行**。

## 组件设计

### C1 `forge_dns_reply`(纯函数,`client_tun.rs`)— TDD 核心
```
fn forge_dns_reply(udp: &UdpInbound, fake_pool: &mut FakeIpPool, now_secs: u64) -> Option<Vec<u8>>
```
- `dns::parse_query(udp.payload)`;不可解析 → `None`(调用方丢弃,app 重查自愈)。
- A 查询 → `fake_pool.alloc(qname, now)` → `Answer::A(ip, ttl=5)`;AAAA/其它 → `Answer::NoData`。
- `build_response` → `build_udp_ip_packet(src=(udp.dst_ip,53), dst=(udp.src_ip,udp.src_port), resp)` → 返回裸 IP/UDP 回包字节。
- 保留 `🪪 DNS {qname} → fake-IP {ip}` / `NODATA` 日志(沿用 `drain_dns` 行为;DNS 低频,非热路径)。
- **纯/半纯**:只依赖 `UdpInbound` + `&mut FakeIpPool`,无 device/async → 单测喂构造的查询包、断言回包 `parse_inbound_udp` 回程 `src=resolver:53`、`dst=app`、含 fake-IP A 记录。

### C2 `classify_inbound` 改为「任意 :53 → Dns」(`client_tun.rs`)
```
Some(udp) => if udp.dst_port == 53 { Inbound::Dns } else { Inbound::UdpRelay }
```
- 去掉 `dst_ip == FAKE_DNS_RESOLVER` 条件(连同 `FAKE_DNS_RESOLVER` 常量一并移除)。`:853`/`:443`/其它 → `UdpRelay`(不变)。

### C3 `handle_dns_hijack`(async I/O 薄壳,`client_tun.rs`)
- rx 分支 `class == Dns` → `rx_take` → `parse_inbound_udp` → `forge_dns_reply` → `inject_ip_packet` + `flush_tx`。**不进 `iface.poll`**(与 `UdpRelay` 同款 take-and-handle)。

### C4 `resolve_target` 加 `port==53 → Block`(`client_tun.rs`)
- 新 `dns_block::is_dns_relay_port(port) -> port == 53 || port == 853`(语义:到达 resolve_target 的 DNS 端口都该 Block——UDP :53 已被 hijack 截走,只剩 TCP :53 + DoT/DoQ :853)。替换现有 `is_encrypted_dns_port` 调用点,注释写死不变量。
- TCP :53 → 既有 `Block → rearm(RST)`;零新 I/O。

### C5 删除清单(`client_tun.rs`)
- `dns_handle` socket 创建 + `bind(198.18.0.1:53)`;接口 IP `198.18.0.1/32`;`drain_dns` 函数 + 两处调用([:577](../../src/client_tun.rs)/[:643](../../src/client_tun.rs));`FAKE_DNS_RESOLVER` 常量。
- fake_ip.rs:36 `.1` 预留注释更新为「预留为可广告的 resolver 地址」。

## 测试边界(诚实分层)

- **纯单元(TDD red→green,主战场)**:
  - `forge_dns_reply`:`8.8.8.8:53` A 查询 → 回包 `src=8.8.8.8:53`/`dst=app`/A=fake-IP;`198.18.0.1:53` 同样伪造;dst 落 fake-IP 段也伪造;AAAA → NODATA;不可解析 → None;同域名稳定复用同 fake-IP。
  - `classify_inbound`:`8.8.8.8:53`/`198.18.0.1:53`/`1.1.1.1:53` → `Dns`;`:853`/`:443` → `UdpRelay`;TCP/垃圾 → `Other`。
  - `resolve_target`:`(任意IP,53)` → `Block`(新);`:853` → `Block`(回归);普通 :443/:80 不 block(零回归)。
  - `is_dns_relay_port`:`53/853→true`、`443/80/0/65535→false`。
- **harness(若成本可控)**:注入一个 inbound `:53` 查询 → 跑主循环 → 捕获 `inject` 的回包 → 断言伪造正确、未走上游。若注入/捕获成本高,降级 acceptance(同刀4 边界)。
- **真出口 acceptance(关键,见 plan T9)**:**系统 DNS 不指 198.18.0.1**(设成 8.8.8.8 或留 DHCP),app 查 `8.8.8.8:53` 仍被本地伪造 fake-IP、仍能上网;client 日志见 `🪪 DNS … → fake-IP`;DoH 关/开回归;TCP :53 被 RST(`dig +tcp @8.8.8.8` 行为)。

## 风险 / 已知边界

- **split-horizon / 内网域名**:全劫持下纯内网域名会走出口解析而失败(已知限制,D4);全隧道下 on-link LAN DNS 不进 TUN,实际罕见。需要时后续加 dst allowlist。
- **exotic DNS 查询丢弃**:`parse_query` 只认单 question、无压缩指针;不可解析的 :53 包**丢弃**(不转发真 DNS,**故意**——转发即泄漏),app 超时重查。主流 getaddrinfo 单 A/AAAA 查询全覆盖。
- **IPv6 / AAAA-over-IPv6**:crate 仅 `proto-ipv4`,`parse_inbound_udp` 只解析 IPv4 → IPv6 :53 不被劫持(落 `Other` → smoltcp(亦 ipv4-only)→ 丢弃)。AAAA(IPv4 传输)已 NODATA。IPv6 数据面整体 defer(移动端 stage)。
- **TCP :53 升级**:我方 UDP 应答永不置 TC,标准 stub 不升级 TCP;硬 TCP-first 的 app 被 RST → 回落 UDP :53。真出口 acceptance 验证。
- **ADR**:裸包拦全 :53 + 废 smoltcp DNS socket 满足 hard-to-reverse + surprising(未来读者会问"为何不沿用 smoltcp DNS socket")+ 真权衡(裸包 vs AnyIP)→ `docs/adr/0007-hijack-all-plaintext-dns.md`(T0 落库)。

# 2026-06-02 Stage 11 fake-IP DNS Spec

## 背景

到 Stage 10，mini_vpn 能透明转发任意 IP:port 的 TCP、断线自愈。但**被 GFW 污染的
域名**（facebook/google 等）仍不可达：本地 DNS 解析返回毒化/错误 IP，客户端从 SYN
提取到的就是错 IP，出口在美国也救不回。

Stage 11 引入 **fake-IP** 模式：本地拦截 DNS、回假 IP、TCP 时查表换回域名、relay 带
域名，让**域名在出口（美国）解析**，绕过本地污染。

关键前提（grounding 已确认）：**出口端零改动** —— `server.rs` 对 `RelayRequest::Tcp
{ target: DomainPort }` 用 `to_wire_string()` + `TcpStream::connect`，会用 server 自己
的干净 DNS 解析域名。relay 协议的 `DomainPort` 变体早已存在。

## 术语（见 CONTEXT.md / ADR-0002）

- **fake-IP**：`198.18.0.0/15` 内分配给域名的占位 IPv4，非真实地址。
- **fake-IP map**：客户端持有的 `domain ↔ fake-IP` 双向表。

## 目标

1. 在 TUN 内用 smoltcp UDP socket 监听 `198.18.0.1:53`，拦截应用 DNS 查询。
2. A 查询 → 分配/复用 fake-IP，伪造 A 响应返回（短 TTL）；AAAA / 其它 → NODATA。
3. TCP SYN 目的 IP ∈ `198.18.0.0/15` → 查 map 得域名 → relay 用 `DomainPort`；
   查不到 → 拒绝该连接（rearm，靠应用重查自愈）。
4. 目的 IP 不在 fake 段 → 保持 `IpPort`（Stage 8/9 行为不变）。
5. server 端零改动。

## 非目标（记入 TODO.md）

- DoH/DoT 拦截（加密 DNS 绕过本地 resolver）。
- 直连硬编码 IP 的域名化（无 DNS 查询，无法映射）。
- QUIC/UDP relay（任务 4）。
- 真实 DNS 转发 / split-DNS / 非 A 记录的真实应答（CNAME/HTTPS/SVCB/MX…）。
- fake-IP 回收 / LRU（13 万地址，本阶段够用）。
- DNS 编解码换 hickory-proto（触发条件见下）。

## 架构边界

### 数据流（fake-IP 全链路）

```text
1. App 要访问 facebook.com → 向系统 DNS(=198.18.0.1) 发 A 查询
2. 198.18.0.0/15 路由进 TUN → smoltcp UDP socket @198.18.0.1:53 收到查询
3. 解析查询 → 分配/复用 fake-IP(如 198.18.0.5) → 记 map → 伪造 A 响应(TTL=5s)回 App
4. App 拿 198.18.0.5 → 向 198.18.0.5:443 发 TCP SYN
5. fake 段路由进 TUN → SYN inspector(Stage 9) 接住 → 提取 target=198.18.0.5:443
6. fake-IP resolve：198.18.0.5 ∈ fake 段 → 查 map → facebook.com → target=DomainPort{facebook.com,443}
7. relay RelayRequest::Tcp{DomainPort} → server
8. server 用干净 DNS 解析 facebook.com → 真实 IP → connect → 字节回传
```

### 模块（对应 5 个 task）

- **T1 `FakeIpPool`**（纯逻辑）：`198.18.0.0/15`，从 `.2` 起环形分配（`.1` 预留 resolver）；
  双向 `HashMap<String, Ipv4Addr>` + `HashMap<Ipv4Addr, String>`；`alloc(domain)->Ipv4Addr`
  同域名稳定复用；`resolve(ip)->Option<String>`；`is_fake(ip)->bool`。无锁，主循环独占。
- **T2 DNS 编解码**（纯逻辑）：`parse_query(&[u8]) -> Option<DnsQuery{id, qname, qtype}>`；
  `build_response(query, answer) -> Vec<u8>`，answer = A(fake-IP, ttl) | NoData。只处理单
  question、无压缩指针的查询；响应 answer 用 `0xC00C` 指针回指 question。
- **T3 DNS 拦截接入**：在 TUN runtime 加一个 smoltcp `udp::Socket` bind `198.18.0.1:53`；
  主循环 poll 后处理 UDP recv → parse_query → (A: pool.alloc + build A) / (else: NODATA) →
  udp send 回 App。
- **T4 target 改写**：Stage 9 提取 `IpEndpoint` 后，若 `pool.is_fake(ip)`：`pool.resolve(ip)`
  得域名 → `TargetAddr::DomainPort`；`None` → 拒绝（不开 relay、rearm）。非 fake → 原样 IpPort。
- **T5**：教学笔记 + 全套校验 + 跨机 e2e（curl 一个被墙域名）。

### smoltcp UDP socket

- features 已开 `socket-udp`。`udp::Socket::new(rx_meta+rx_payload, tx_meta+tx_payload)`，
  `bind(IpListenEndpoint{ addr: None 或 198.18.0.1, port: 53 })`。AnyIP 已开，可收发往
  `198.18.0.1` 的包。`recv() -> (&[u8], UdpMetadata{ endpoint })`，`send_slice(data, endpoint)`。

## 失败语义

- DNS parse 失败 / 非单 question / 有压缩指针 → 不响应（让 App 超时重试或换查询）。不 panic。
- fake-IP 池耗尽（极不可能，13 万）→ 日志告警，该查询 NODATA。
- TCP 到 fake 段但 map 无记录 → 拒绝连接（rearm），不 panic。
- 任何 DNS/UDP 路径错误都不得使主循环 panic 或退出。

## 已知边界（必须对用户透明，见 ADR-0002）

1. **DoH/DoT**：应用加密 DNS 不走明文 UDP/53 → 拦不到 → 真实 IP 直连（IpPort），被墙 IP 仍失败。
2. **直连硬编码 IP**：无 DNS → 不进 map → IpPort relay（走隧道但无出口解析好处）。
3. **QUIC/UDP**：本阶段无 UDP relay；应用通常 happy-eyeballs 回退 TCP。

## 日志

- `🪪 DNS 查询 {qname} ({qtype}) → fake-IP {ip}` / `→ NODATA`
- `🔁 fake-IP {ip} resolve → {domain}`
- `🚫 fake-IP {ip} 无映射，拒绝连接（请重新解析）`

## 测试策略（详见 plan）

- T1：alloc 稳定复用、is_fake 边界（段内/段外/.1 预留）、resolve 命中/未命中、环形分配。
- T2：parse 真实抓包的 A/AAAA 查询字节、build A 响应 round-trip、NODATA 响应字段（QR=1,ANCOUNT=0）。
- T3/T4：集成层，靠手动 e2e。
- e2e：跨机，curl 一个被墙域名（如 `https://www.facebook.com`，需系统 DNS 指 198.18.0.1 +
  198.18.0.0/15 路由进 utun），观察 DNS→fake-IP→DomainPort→出口解析→200/重定向。

## 文件范围

- `src/client_tun.rs`（pool、dns 编解码、UDP 拦截、target 改写、测试）或拆出 `src/fake_ip.rs` / `src/dns.rs`
- `docs/tech/2026-06-02-stage-11-fake-ip-dns-plan.md`
- `docs/tech/11-fake-ip-dns.md`（教学笔记）
- `CONTEXT.md`（已加 fake-IP 术语）、`docs/adr/0002-*`（已建）、`TODO.md`

## 验收标准

1. 系统 DNS 指 `198.18.0.1` + `198.18.0.0/15` 路由进 utun 后，`curl` 一个被墙域名经隧道可达
   （client 日志显示 DNS→fake-IP→resolve→DomainPort，server 日志显示域名 connect）。
2. 直连真实 IP（`curl 1.1.1.1`）仍按 IpPort 工作（不回归）。
3. 客户端重启后用旧 fake-IP 的连接被干净拒绝、应用重查自愈、不 panic。
4. `cargo test` / `check` / `clippy --all-targets --all-features -- -D warnings` / `doc --no-deps`
   全过；CI 双平台绿。

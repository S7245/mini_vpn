# 11 fake-IP DNS

## 背景

Stage 10 之前，mini_vpn 能转发任意 IP:port 的 TCP，但被 GFW 污染的域名仍不可达：
本地 DNS 把 `facebook.com` 解析成毒化 IP，客户端从 SYN 提取到的就是错 IP，出口在美国
也救不回。Stage 11 用 fake-IP 模式让域名在出口解析，绕过本地污染。

关键前提：出口端零改动。`server.rs` 对 `RelayRequest::Tcp { target: DomainPort }`
直接 `TcpStream::connect("host:port")`，用 server 自己的干净 DNS 解析。客户端只要把域名
塞进 target，server 就会在干净网络解析+连接。

## 全链路数据流

```text
1. App 访问 facebook.com → 向系统 DNS(=198.18.0.1) 发 A 查询
2. 198.18.0.0/15 路由进 TUN → smoltcp UDP socket@:53 收到查询
3. drain_dns 解析 → FakeIpPool.alloc=198.18.0.5 → 伪造 A 响应(ttl=5)回 App
4. App 拿 198.18.0.5 → 向 198.18.0.5:443 发 TCP SYN
5. fake 段路由进 TUN → SYN inspector(Stage 9) 接住 → endpoint=198.18.0.5:443
6. resolve_target: 198.18.0.5 ∈ fake 段 → resolve → DomainPort{facebook.com,443}
7. relay RelayRequest::Tcp{DomainPort} → server
8. server 用干净 DNS 解析 facebook.com → 真实 IP → connect → 字节回传
```

## 模块

- `src/fake_ip.rs` `FakeIpPool`：198.18.0.0/15，从 .2 起环形分配(.1 预留 resolver)，
  双向 domain<->fake-IP，同域名稳定复用，is_fake/resolve/alloc。无锁，主循环独占。
- `src/dns.rs`：手写最小 DNS 编解码。parse_query(单 question、无压缩指针，越界返回 None
  不 panic)；build_response(A 记录用 0xC00C 指针回指 question，或 NODATA)。
- `client_tun.rs`：drain_dns(poll 后排空 UDP，A->fake-IP，AAAA/其它->NODATA，再 poll
  一次把响应发出)；resolve_target(fake->DomainPort，非 fake->IpPort，fake 无映射->Refuse)。

## 为什么 drain_dns 后要再 poll 一次

iface.poll 双向搬运：收包填 socket rx buffer、把 socket tx buffer 变 IP 包入 device 发货
队列。drain_dns 在第一次 poll 后从 rx buffer 读查询、send_slice 响应到 tx buffer——响应此刻
还在 buffer 里。必须再 poll 一次才能把它变成 IP 包进 device tx_queue，flush_tx 才发得出去。
漏掉这次 poll，DNS 响应永远发不出，应用 DNS 超时。

## DNS 应答策略

- A -> fake-IP。
- AAAA -> NODATA(rcode=0、0 答案)。只做 IPv4 fake-IP，用 NODATA 逼双栈应用退回 IPv4。
  绝不能对 AAAA 不响应——会让应用等超时、拖慢首屏。
- 其它(HTTPS/SVCB/MX…) -> NODATA。让应用干净退回，不挂起。
- 全部本地应答，不外发任何真实 DNS 查询。

## 边缘：fake-IP 段内但查不到映射

客户端重启后映射表清空，但应用 DNS 缓存里还存旧 fake-IP，于是用旧 fake-IP 发 TCP，查不到
域名 -> resolve_target 返回 Refuse -> 复位该 socket(rearm) -> 应用 TCP 失败 -> 重新 DNS
查询 -> 拿到新 fake-IP。配合 fake 响应短 TTL(5s) 缩小陈旧窗口。靠上层 TCP 自愈。

## 已知盲点（见 ADR-0002 / TODO.md）

1. DoH/DoT 加密 DNS：浏览器/系统加密 DNS 走 443，不经明文 UDP/53 -> 拦不到 -> 应用拿真实
   IP 直连(IpPort)，被墙 IP 仍失败。缓解：劫持已知 DoH 端点 / 关应用内 DoH。
2. 直连硬编码 IP：无 DNS 查询 -> 不进 fake-IP 表 -> IpPort relay。
3. QUIC/UDP：本阶段无 UDP relay；应用通常 happy-eyeballs 回退 TCP。

## 验收 recipe（跨机，需 sudo / 改系统 DNS）

US Upstream + 深圳客户端按 Stage 9/10 起好。然后在客户端：

```bash
UT=$(ifconfig | awk '/^utun/{i=$1} /inet 10\.0\.0\.1 /{print i}' | tr -d ':')
echo "utun = $UT"
# 1) fake-IP 段路由进 utun（DNS 与 fake-IP 流量一起收进来）
sudo route -n add -net 198.18.0.0/15 -interface "$UT"
# 2) 系统 DNS 指向 fake resolver（用完务必恢复！）
networksetup -setdnsservers Wi-Fi 198.18.0.1

curl -v https://www.facebook.com/

# 3) 恢复 DNS（重要）
networksetup -setdnsservers Wi-Fi empty
sudo route -n delete -net 198.18.0.0/15
```

预期客户端日志：`🪪 DNS www.facebook.com (A) → fake-IP 198.18.0.x`、
`🔁 fake-IP 198.18.0.x resolve → www.facebook.com`、relay 用 DomainPort。
预期 server 日志：`解析出的目标地址是: www.facebook.com:443`。
curl 收到 facebook 真实 TLS 响应（证明出口端解析绕过本地污染）。
直连真实 IP（curl https://1.1.1.1/）仍按 IpPort 工作，不回归。

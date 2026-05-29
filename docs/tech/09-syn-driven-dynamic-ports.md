# 09 SYN-Driven Dynamic Ports

## 背景

Stage 8 让 TUN 链路能接受任意目的 IP，但 smoltcp 的 `listen(port)` 一个 socket
只认一个端口。Stage 8 监听端口固定（默认 80），发往 443 / 任意其它端口的 SYN
都被静默丢弃。Stage 9 解决这一限制。

中文要点：Stage 8 是"IP 任意"，Stage 9 是"端口任意"——合起来才是真正的透明 TCP 代理。

## 机制：rx 热路径的 SYN inspector + 动态监听池

每当 TUN 设备读出一帧，**在 `iface.poll` 之前**做一次纯解析：

```rust
fn inspect_inbound_syn(packet: &[u8]) -> Option<u16> {
    let parsed = etherparse::PacketHeaders::from_ip_slice(packet).ok()?;
    let etherparse::TransportHeader::Tcp(tcp) = parsed.transport? else {
        return None;
    };
    if tcp.syn && !tcp.ack { Some(tcp.destination_port) } else { None }
}
```

- 仅干净 SYN（`syn && !ack`）返回 `Some(dst_port)`。SYN-ACK / 普通 ACK / 非 TCP / 解析失败一律 `None`。
- 不修改 packet、不持有状态。

得到 `dst_port` 后调用：

```rust
registry.ensure_port(port, &mut sockets, &mut socket_ctxs);
```

`ListenerRegistry` 内部：

- `ports: HashMap<u16, Vec<SocketHandle>>` — 一个端口对应一组 `pool_size` 个监听 socket。
- 端口已在册时 `ensure_port` 幂等返回 `Ok(())`。
- 端口数超过 `MAX_INTERCEPTED_PORTS = 64` 时返回 `RegistryError::Capped`，
  日志告警，**已注册端口不受影响、不 panic**。

因为 SYN inspector 跑在 `iface.poll` 之前，**同一帧** smoltcp 就能 accept 这个 SYN，
不会出现"先丢一次 SYN、curl 重传后才被接住"的延迟。

## 主循环改造

```text
device.wait_for_rx()
↓
inspect_inbound_syn(buf) -> 若是新端口 SYN 调用 ensure_port
↓
iface.poll(...)
↓
device.flush_tx().await
↓
for handle in registry.all_handles() { process_listener_activity(handle, ...) }
```

`process_listener_activity` 内部沿用 Stage 8 的逻辑：取首包 → `local_endpoint()` →
`target_from_endpoint` → `handle_local_payload` 开远端 yamux 子流。

## 配置变化

| 项 | Stage 8 | Stage 9 |
|---|---|---|
| `MINI_VPN_TUN_LOCAL_PORT` | 默认 80 | **删除**（端口动态学习） |
| `MINI_VPN_TUN_POOL_SIZE` | 默认 4（全局） | 默认 **2**（每端口） |
| `MAX_INTERCEPTED_PORTS` | n/a | 编译期常量 **64** |
| `MINI_VPN_TUN_SERVER_ADDR` / `TLS_SNI` / `CA_PATH` | 保持 | 保持 |

启动 banner 也去掉了 `local_port=` 字段：

```text
🚀 TUN runtime started with pool_size=2, server_addr=..., tls_sni=..., ca_path=...
```

## 内存估算

`pool_size × MAX_INTERCEPTED_PORTS × 2 × TCP_SOCKET_BUFFER_SIZE`
= 2 × 64 × 2 × 64KB ≈ **16 MB** 上限，可控。

## 验收 recipe（跨机拓扑，沿用 Stage 8）

> 须 Upstream 在另一台机器（沿用 Stage 8 的拓扑要求；同机会被全局路由劫持出口）。

**通用准备**（每次测试前都做）：
```bash
# US 服务器（保持运行）
MINI_VPN_SERVER_BIND_ADDR=0.0.0.0:8081 \
MINI_VPN_SERVER_CERT_PATH=certs/dev/server-cert.pem \
MINI_VPN_SERVER_KEY_PATH=certs/dev/server-key.pem \
./target/debug/mini_vpn server

# 深圳客户端
sudo \
  MINI_VPN_TUN_SERVER_ADDR=<US_IP>:8081 \
  MINI_VPN_TUN_TLS_SNI=example.com \
  MINI_VPN_TUN_CA_PATH=certs/dev/ca-cert.pem \
  ./target/debug/mini_vpn client-tun 2>&1 | tee /tmp/mv-client.log

# 启动行应该长这样（注意：没有 local_port= 了）：
# 🚀 TUN runtime started with pool_size=2, server_addr=<US_IP>:8081, tls_sni=example.com, ca_path=certs/dev/ca-cert.pem
```

**端口 80 测试**：
```bash
UT=$(ifconfig | awk '/^utun/{i=$1} /inet 10\.0\.0\.1 /{print i}' | tr -d ':')
sudo route -n add -host 1.1.1.1 -interface "$UT"
curl -v -m 15 http://1.1.1.1/
sudo route -n delete -host 1.1.1.1
```

预期：
- client 日志出现 `🆕 listener pool created for port 80 (pool_size=2)`
- `🎯 ... extracted target 1.1.1.1:80`
- server 日志 `解析出的目标地址是: 1.1.1.1:80`
- curl 收到 `< HTTP/1.1 301`

**端口 443 测试**（**关键**——证明任意端口）：
```bash
UT=$(ifconfig | awk '/^utun/{i=$1} /inet 10\.0\.0\.1 /{print i}' | tr -d ':')
sudo route -n add -host 1.1.1.1 -interface "$UT"
curl -v -k -m 15 https://1.1.1.1/
sudo route -n delete -host 1.1.1.1
```

预期：
- client 日志 `🆕 listener pool created for port 443 (pool_size=2)`
- `🎯 ... extracted target 1.1.1.1:443`
- server 日志 `解析出的目标地址是: 1.1.1.1:443`
- curl 收到 Cloudflare HTTPS 响应（HTTP/1.1 或 HTTP/2）

`-k` 是因为我们直接连 IP 字面量，Cloudflare 证书 SAN 不含 IP，不验证证书。
**这跟我们的隧道无关**——隧道只搬字节，TLS ClientHello 由 curl 生成、
随流量到达 1.1.1.1，Cloudflare 按 SNI（curl 自动填 `Host: 1.1.1.1` 不是合法 SNI，
所以 Cloudflare 用默认证书；这一切在我们隧道外面发生）。

## 距离"真能上网"还差什么

到 Stage 9 为止：任意 IP + 任意端口的 TCP 都能透明转发。剩下：

1. **DNS over tunnel / fake-IP**：被墙域名（facebook 等）本地解析失败，需要走隧道解析或假 IP 映射。
2. **UDP relay**（QUIC / 直播）。
3. **出口 IP 质量**（datacenter 风控）——协议无关。
4. **MSS clamping / MTU**（大包不卡死）。

详见 `TODO.md` 的 "Gating dependencies" 与 "Future architecture topics"。

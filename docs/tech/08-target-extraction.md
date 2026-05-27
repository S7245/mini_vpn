# 08 Target Extraction

## 背景

Stage 7 之前，TUN 客户端把每个被拦截连接都转发到一个**写死的** Target
（`httpbin.org:80`）。它还不是代理，只是一条固定隧道。

Stage 8 把 Target 改成**从被拦截连接本身提取**：你想连哪，它就转发到哪。

中文要点：这是 TUN 链路第一次具备真实的目标路由能力，但仅限单一固定监听端口（任意端口见 Stage 9）。

## 术语（见 CONTEXT.md）

- **Upstream**：客户端穿过的代理/中继服务器（美国服务器）。
- **Target**：被拦截连接真正想去的 `IP:port`。本阶段从 smoltcp 的 `local_endpoint()` 提取。

## Target 为什么永远是 IP:port（见 ADR-0001）

在 IP 层，操作系统**发包前就完成了 DNS 解析**。等一个 TCP SYN 进入 TUN、被 smoltcp 看到时，
包里只有目的 **IP:port**，没有域名。所以提取出的 Target 永远是 `TargetAddr::IpPort`，
TUN 链路永不产生 `TargetAddr::DomainPort`（域名形态只存在于 SOCKS5 的 `client-direct` 路径）。

- Upstream 按 IP 出网即可；HTTPS 仍然正常，因为客户端 TLS 的 SNI 随字节流在带内传递。
- 隧道**不需要、也不会**携带域名。

提取实现是一个纯函数：

```rust
fn target_from_endpoint(endpoint: smoltcp::wire::IpEndpoint) -> TargetAddr {
    let ip = std::net::IpAddr::from(endpoint.addr);
    TargetAddr::IpPort(std::net::SocketAddr::new(ip, endpoint.port))
}
```

## AnyIP：为什么默认路由的网关填本机自己的 IP

默认情况下 smoltcp 只接收目的 IP = 接口自身地址（10.0.0.2）的包。要接收"任意目的 IP"
（即真实 Target），需要两步：

```rust
iface.set_any_ip(true);
iface
    .routes_mut()
    .add_default_ipv4_route(smoltcp::wire::Ipv4Address::new(10, 0, 0, 2))
    .unwrap();
```

第二步反直觉：**默认路由的网关填的是本接口自己的 IP `10.0.0.2`**。
原因是 smoltcp 的 AnyIP 接收判定不是"无脑收所有包"，而是要求
`routes.lookup(dst)` 返回一个本接口的 IP 才放行（见 smoltcp `iface/interface/ipv4.rs`）。
所以网关=自己不是笔误，而是 AnyIP 的硬性要求。

## 提取时机

- **首个本地 payload 到达时**触发（沿用既有开远端时机）。
- 在 `process_listener_activity` 持有 socket 时，取首包的同时读 `local_endpoint()`，
  把 Target 传给 `handle_local_payload` 的"开远端"分支。
- **已知局限**：连接建立后客户端不先发数据的协议（server-speaks-first）永不开远端。
  这是 pre-existing 限制，Stage 8 未改变（记录在 TODO.md）。

## 单一固定监听端口（Stage 9 才解除）

smoltcp 的 `listen(port)` 一个 socket 只认一个端口。Stage 8 仍监听单一端口
（`MINI_VPN_TUN_LOCAL_PORT`，默认 80），所以提取出的 **IP 任意、但 port = 监听端口**。

要支持任意端口（含 443/HTTPS），需在收包热路径嗅探 SYN 并动态创建对应端口的监听 socket
——这是 Stage 9。

## 配置变化

移除：`MINI_VPN_TUN_TARGET_ADDR`（及内部 `DEFAULT_TUN_TARGET` / `TunListenerConfig.target_addr` /
`SocketCtx.target`）。Target 不再可配置，一律运行时提取。

保留：`MINI_VPN_TUN_LOCAL_PORT`、`MINI_VPN_TUN_POOL_SIZE`、`MINI_VPN_TUN_SERVER_ADDR`、
`MINI_VPN_TUN_TLS_SNI`、`MINI_VPN_TUN_CA_PATH`。

## 验收 recipe（手动，需要 sudo / TUN）

> 靶子用 **IP 字面量**，不要用域名——很多开发机装了 TUN 代理（Clash/Mihomo）会
> 劫持 DNS 发 fake-ip（`198.18.0.0/15`），用域名会被解析成假 IP，污染测试。
> 选 `1.1.1.1:80`（稳定、返回 301、免 DNS）。不要用 `93.184.216.34`（已失效）。

**Step 0 — preflight：先确认靶子不走我们的 VPN 时是活的**

```bash
curl -sS -m 8 -o /dev/null -w "http_code=%{http_code}\n" http://1.1.1.1/   # 期望 301
```

若这一步就失败，问题在靶子/网络，与本项目无关，先换靶子。

**Step 1 — 构建**：`cargo build`

**Step 2 — 终端 A 起 Upstream（保留日志）**：

```bash
cargo run -- server 2>&1 | tee /tmp/mv-server.log
```

**Step 3 — 终端 B 起 client-tun（保留日志）**：

```bash
sudo -E ./target/debug/mini_vpn client-tun 2>&1 | tee /tmp/mv-client.log
```

**Step 4 — 终端 C：取本项目的 utun 号并路由靶子进去**（10.0.0.1 是本项目 utun 的地址）：

```bash
UT=$(ifconfig | awk '/^utun/{i=$1} /inet 10\.0\.0\.1 /{print i}' | tr -d ':')
echo "mini_vpn utun = $UT"
sudo route -n add -host 1.1.1.1 -interface "$UT"   # /32 host route，比其它代理的路由更具体，会胜出
```

**Step 5 — 触发请求（抓 verbose）**：

```bash
curl -v -m 15 http://1.1.1.1/ 2>&1 | tee /tmp/mv-curl.log
```

**Step 6 — 预期**：
- curl：`< HTTP/1.1 301 Moved Permanently`（非空回包）
- client 日志：`🎯 handle ... extracted target 1.1.1.1:80`
- server 日志：`解析出的目标地址是: 1.1.1.1:80` 且成功连上

这同时验证：AnyIP 让任意目的 IP 进栈 ✅、Target 从 `local_endpoint` 真实提取 ✅、
Upstream 按提取 IP 出网 ✅。

**Step 7 — 收尾**：

```bash
sudo route -n delete -host 1.1.1.1
# Ctrl-C 关掉终端 A/B 的 server 与 client-tun
```

**全本地确定性变体（无外网、避开竞争代理）**：用本机 HTTP server 当靶子。
设 `MINI_VPN_TUN_LOCAL_PORT=8080`（提取出的 port = 监听端口），加 lo0 别名让 Upstream 能连到提取出的 IP：

```bash
sudo ifconfig lo0 alias 198.51.100.10           # 给本机加一个测试 IP
python3 -m http.server 8080 &                    # 监听 0.0.0.0:8080，覆盖该别名
# server: cargo run -- server
# client: sudo -E MINI_VPN_TUN_LOCAL_PORT=8080 ./target/debug/mini_vpn client-tun
UT=$(ifconfig | awk '/^utun/{i=$1} /inet 10\.0\.0\.1 /{print i}' | tr -d ':')
sudo route -n add -host 198.51.100.10 -interface "$UT"
curl -v http://198.51.100.10:8080/               # 期望返回目录列表，client/server 日志显示 target 198.51.100.10:8080
```

## 拓扑要求：Upstream 必须在另一台机器（否则字节回不来）

`route add -host <target> -interface <utun>` 是**全机生效**的。若 Upstream（server）
跑在与 client-tun **同一台机器**上，server 自己 `connect(<target>)` 的出站也会被这条
路由拽进本机 TUN，到不了真实目标 → `Connection refused` / 回环。

- **Target 提取本身在同机即可验证**（client 日志的 `🎯 extracted target` + server 日志的
  `解析出的目标地址是`）。
- **完整字节往返必须把 Upstream 放到另一台机器**（真 VPN 拓扑，例如美国服务器）。

### 跨机往返测试（美国服务器当 Upstream）

US 服务器上（构建后）：

```bash
MINI_VPN_SERVER_BIND_ADDR=0.0.0.0:8081 \
MINI_VPN_SERVER_CERT_PATH=certs/dev/server-cert.pem \
MINI_VPN_SERVER_KEY_PATH=certs/dev/server-key.pem \
./mini_vpn server
```

客户端（深圳 Mac；tls_sni 取证书 SAN 里有的名字，证书与连接 IP 解耦）：

```bash
MINI_VPN_TUN_SERVER_ADDR=<US_IP>:8081 \
MINI_VPN_TUN_TLS_SNI=example.com \
MINI_VPN_TUN_CA_PATH=certs/dev/ca-cert.pem \
sudo -E ./target/debug/mini_vpn client-tun 2>&1 | tee /tmp/mv-client.log

UT=$(ifconfig | awk '/^utun/{i=$1} /inet 10\.0\.0\.1 /{print i}' | tr -d ':')
sudo route -n add -host 1.1.1.1 -interface "$UT"
curl -v -m 15 http://1.1.1.1/        # 期望 301，server 在 US 机出网不再被本机路由劫持
sudo route -n delete -host 1.1.1.1
```

## 排障：`curl: (52) Empty reply from server`

TCP 连接被接受了但没收到任何应用层字节。逐层定位：

| 现象 | 含义 / 下一步 |
|---|---|
| preflight（Step 0）就 52 | 靶子本身死了，换靶子（如 `1.1.1.1`） |
| client 日志无 `📡 收到来自操作系统的包` | 路由没生效/进错了别的 utun；核对 Step 4 的 `$UT` |
| client 有收包但无 `🎯 extracted target` | smoltcp 没完成握手或 `local_endpoint` 为空；查 any_ip/默认路由 |
| client 有 `🎯` 但 server 无 `解析出的目标地址` | yamux/TLS 链路问题，看 server 日志握手 |
| server 打印 `无法连接到目标地址` | Upstream 出网受阻（可能被竞争代理 utun1024 拦），换靶子或临时关掉竞争代理 |
| 域名解析成 `198.18.x.x` | 竞争 TUN 代理的 fake-ip 在劫持 DNS；用 IP 字面量，别用域名 |

## 距离"真能上网"还差什么（见 TODO.md）

Target 提取只是第一块基石。要 browse 真实站点（尤其墙内被封域名），还依赖：
任意端口（Stage 9）、DNS over tunnel / fake-IP、UDP relay（QUIC/直播）、出口 IP 质量、MSS clamping。

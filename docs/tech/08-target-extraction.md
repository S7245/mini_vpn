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

1. 选稳定 HTTP Target：`example.com` = `93.184.216.34:80`。
2. 构建：`cargo build`。
3. 终端 1 起 Upstream（确保它能出网连 example.com:80）：

   ```bash
   cargo run -- server
   ```

4. 终端 2 起 client-tun（默认监听 80）：

   ```bash
   sudo -E ./target/debug/mini_vpn client-tun
   ```

5. `ifconfig` 找到 utun 号，把目标 IP 路由进 utun：

   ```bash
   sudo route add -host 93.184.216.34 -interface utun<N>
   ```

6. 触发请求：

   ```bash
   curl http://93.184.216.34/
   ```

7. 预期：
   - client 打印 `🎯 handle ... extracted target 93.184.216.34:80`
   - server 打印 `解析出的目标地址是: 93.184.216.34:80` 并返回 example.com 的 HTML

这同时验证：AnyIP 让任意目的 IP 进栈 ✅、Target 从 `local_endpoint` 真实提取 ✅、
Upstream 按提取 IP 出网 ✅。

## 距离"真能上网"还差什么（见 TODO.md）

Target 提取只是第一块基石。要 browse 真实站点（尤其墙内被封域名），还依赖：
任意端口（Stage 9）、DNS over tunnel / fake-IP、UDP relay（QUIC/直播）、出口 IP 质量、MSS clamping。

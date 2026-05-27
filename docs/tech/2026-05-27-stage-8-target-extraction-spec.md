# 2026-05-27 Stage 8 Target Extraction Spec

## 背景

到 Stage 7 为止，TUN 客户端能把被拦截的 TCP 连接经 TLS + Yamux 转发给 Upstream（代理服务器），
但转发的 **Target**（最终目的地）仍然是写死的：

- `DEFAULT_TUN_TARGET = "httpbin.org:80"`
- 每个监听槽位的 `SocketCtx.target` 都被塞进这个固定值
- `handle_local_payload` 用 `ctx.target` 构造 `RelayRequest::Tcp { target }`

这意味着无论用户实际想访问哪里，Upstream 永远去连同一个地址——它还不是一个真正的代理，只是一条固定隧道。

中文要点：Stage 8 把"转发去哪"从写死改成"从被拦截连接本身提取"，让 TUN 链路第一次具备真正的目标路由能力。

## 术语（见 CONTEXT.md）

- **Upstream**：客户端穿过的代理/中继服务器（美国服务器），与之建立 TLS + Yamux。
- **Target**：被拦截连接想抵达的真实 `IP:port`。本阶段从 smoltcp 的 `local_endpoint()` 提取。

## 目标

Stage 8 的最小目标：

1. 移除写死的 Target 配置：`DEFAULT_TUN_TARGET`、`MINI_VPN_TUN_TARGET_ADDR`、`TunListenerConfig.target_addr`、`SocketCtx.target`。
2. 开启 smoltcp AnyIP，使目的 IP ≠ 接口自身地址的包也能进栈：
   - `iface.set_any_ip(true)`
   - `iface.routes_mut().add_default_ipv4_route(Ipv4Address(10, 0, 0, 2))`（网关 = 本接口 IP，AnyIP 接收判定的要求）
3. 在 `process_listener_activity` 取首包的同时读 `local_endpoint()`，转成 `TargetAddr::IpPort`，作为该连接的 Target。
4. `handle_local_payload` 用提取出的 Target 构造 `RelayRequest::Tcp { target }`，不再用配置值。
5. 保留单一固定监听端口配置 `MINI_VPN_TUN_LOCAL_PORT`（默认 80）。

## 非目标（见 TODO.md）

本阶段明确不做：

- 任意端口（SYN 嗅探 + 动态创建监听 socket）——留给 Stage 9。
- DNS over tunnel / fake-IP——被封域名的正确解析依赖它，但本阶段不碰（见 ADR-0001）。
- UDP relay（QUIC / 视频直播）。
- 连接 Established 即开远端（修 server-speaks-first）——保持首包触发。
- MSS clamping / MTU 调优。

中文要点：本阶段只解决"Target 从哪来"，不解决"任意端口/域名/UDP/出口质量"。

## 架构边界

### Target 形态：恒为 IP:port（ADR-0001）

在 IP 层，操作系统在发包前已完成 DNS 解析，smoltcp 只会看到目的 IP:port。
因此 TUN 链路提取出的 Target 永远是 `TargetAddr::IpPort`，永不出现 `TargetAddr::DomainPort`。
Upstream 按 IP 出网；HTTPS 仍可用，因为客户端 TLS 的 SNI 随字节流在带内传递。

### 提取点与时机

- 时机：**首个本地 payload 到达时**（沿用现有开远端时机）。
- 位置：`process_listener_activity` 持有 socket，在取 payload 的同时读 `socket.local_endpoint()`，
  把提取出的 Target 传给 `handle_local_payload`。
- 已知局限：连接 Established 但客户端不先发数据的协议永不开远端（pre-existing，记入 TODO.md）。

### 配置层变化

```text
TunListenerConfig
├── local_port      （保留）
└── pool_size       （保留）
    target_addr      （删除）
```

`TunRuntimeConfig::from_sources` 去掉 `target_addr` 参数；`from_env` 去掉 `MINI_VPN_TUN_TARGET_ADDR`。

## 启动流

```text
start_tun_proxy()
-> TunRuntimeConfig::from_env()           # 不再读 target_addr
-> build TLS ClientConfig (CA from Stage 7)
-> create_tun_device() (10.0.0.1 / peer 10.0.0.2)
-> VirtualTunDevice::new
-> SocketSet + build_listener_pool        # SocketCtx 不再带 target
-> Interface::new
   -> update_ip_addrs(10.0.0.2/24)
   -> set_any_ip(true)                    # 新增
   -> routes_mut().add_default_ipv4_route(10.0.0.2)  # 新增
-> connect Upstream + TLS + Yamux
-> loop { select! rx / timer / global_rx }
   -> process_listener_activity: extract payload + local_endpoint -> Target
   -> handle_local_payload(target): RelayRequest::Tcp { target } -> open_remote_session
```

## Target 提取语义

新增纯函数（可单测）：

```text
target_from_endpoint(endpoint: IpEndpoint) -> TargetAddr
```

- 输入：smoltcp `IpEndpoint`（`local_endpoint()` 的返回）。
- 输出：`TargetAddr::IpPort(SocketAddr)`。
- 仅支持 IPv4（当前 crate features 只开 `proto-ipv4`）。

## 校验与失败语义

- `local_endpoint()` 返回 `None`（理论上首包时连接已 Established，不应发生）：跳过本次开远端，记录日志，不 panic。
- 提取出的 Target 指向接口自身（10.0.0.1/10.0.0.2）：属于异常自指流量，按失败处理并记录；正常 AnyIP 流量不会落到这里。

中文要点：提取失败必须可观测、不 panic，热路径里不允许 `unwrap()` 在 `local_endpoint` 上炸。

## 日志与可观测性

启动日志去掉 `target=...`（已无固定 target），改为在每条连接开远端时打印提取出的 Target：

```text
🎯 handle {:?} extracted target {ip:port}
```

## 测试策略

### 单元测试

- `target_from_endpoint` 把 IPv4 `IpEndpoint` 正确转为 `TargetAddr::IpPort`。
- 现有 `TunRuntimeConfig` 测试更新：去掉 `target_addr` 相关断言与入参。

### 本机手动联调（验收 recipe）

1. 选稳定 HTTP Target：`example.com` = `93.184.216.34:80`。
2. 起 Upstream（server），确保它能出网连 `example.com:80`。
3. `sudo -E ./target/debug/mini_vpn client-tun`（默认监听 80）。
4. `ifconfig` 取 utun 号，`sudo route add -host 93.184.216.34 -interface utun<N>`。
5. `curl http://93.184.216.34/`。
6. 预期：client 打印提取出的 Target `93.184.216.34:80`；server 打印"解析出的目标地址是: 93.184.216.34:80"并返回 example.com 的 HTML。

## 文件范围

预计涉及：

- `src/client_tun.rs`（配置裁剪、AnyIP 接线、提取函数、提取点改造、测试更新）
- `docs/tech/2026-05-27-stage-8-target-extraction-plan.md`
- `docs/tech/08-target-extraction.md`（教学笔记）
- `CONTEXT.md` / `docs/adr/0001-tun-target-is-ip-port.md` / `TODO.md`（已在 grill 阶段更新）

## 验收标准

Stage 8 完成时应满足：

1. 代码中不再存在任何写死的 TUN Target。
2. 路由一个真实 port-80 主机进 utun 后，Upstream 实际连到该真实 IP 并返回内容。
3. `cargo test`
4. `cargo check`
5. `cargo clippy --all-targets --all-features -- -D warnings`
6. `cargo doc --no-deps`

全部通过。

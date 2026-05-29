# 2026-05-29 Stage 9 SYN-Driven Dynamic Ports Spec

## 背景

Stage 8 把 TUN 链路的 Target 改成了运行时提取，但还有一个根本限制：smoltcp 的
`listen(port)` 一个 socket 只认一个端口。当前 client-tun 只在 `MINI_VPN_TUN_LOCAL_PORT`
（默认 80）上 listen，发往其它端口（443/HTTPS 是最关键的）的 SYN 都被 smoltcp 静默丢弃。

要支持任意端口，必须在收包热路径**嗅探 SYN**：每当 inbound TCP SYN 到来，按其目的端口
动态确保有 smoltcp 监听 socket 在等。

中文要点：Stage 8 是"目的 IP 可任意"，Stage 9 是"目的端口也可任意"——合起来才是真正的
透明 TCP 代理。

## 术语（见 CONTEXT.md）

- **Upstream / Target**：沿用 Stage 8 定义。
- **Port pool**：针对单个目的端口预创建的若干个 smoltcp 监听 socket，用于并发拦截
  同一端口上的多条连接（沿用 Stage 8 的 4 槽位模型，单位从"全局"变成"每端口"）。
- **Listener registry**：`port -> Vec<SocketHandle>` 的总册，主循环遍历所有 handle。

## 目标

1. 在收包热路径加入 **SYN inspector**：对每个 inbound packet，解析 IPv4+TCP，
   判定是否为 `syn && !ack`，若是则提取 `dst_port`。
2. **按需创建监听 socket**：见到一个还没有监听的端口的 SYN，立即为该端口创建一组
   `pool_size` 个 smoltcp 监听 socket，注册进 `Listener registry`，然后再让 `iface.poll`
   处理这一帧——这样 SYN 被同一帧的 smoltcp 接收到。
3. **保留 Stage 8 的目的 IP 提取**：每条新连接首包到达时仍走 `local_endpoint() →
   target_from_endpoint`，自然得到完整的 `IP:port` Target。
4. 删除 `MINI_VPN_TUN_LOCAL_PORT` 和 `DEFAULT_TUN_LISTEN_PORT`（"固定端口"概念已死）。
5. 加 **端口数上限** `MAX_INTERCEPTED_PORTS`（编译期常量，默认 64）防止 SYN flood
   下内存膨胀。
6. 保留每端口 pool（沿用 `MINI_VPN_TUN_POOL_SIZE`，默认从 4 降到 2，64 端口 × 2 槽 × 2 缓冲 ≈ 16MB）。

## 非目标（见 TODO.md）

- **空闲端口回收**：用过的端口的 pool 留在 registry 里直到进程退出（rearm 模式让单槽
  循环可用，不会积累 socket，只积累 port 条目）。回收作为未来优化。
- **IPv6**：不动，crate features 只开 `proto-ipv4`。
- **UDP**：UDP relay 已有协议入口但不走 SYN-driven 路径，本阶段不动。
- **DNS / fake-IP**：不在此阶段；Stage 9 解决的是端口约束，不是域名解析。

## 架构边界

### 当前收包路径

```text
device.wait_for_rx() -> device.rx_buffer = Some(<IP packet>)
iface.poll(...)                      # smoltcp 消费 rx_buffer
device.flush_tx().await              # smoltcp 产生的回包写回网卡
for handle in listener_pool.handles {
    process_listener_activity(...)   # 取首包 + 提取 Target + 开远端
}
```

### Stage 9 新增

```text
device.wait_for_rx() -> device.rx_buffer = Some(<IP packet>)
↓
inspect_inbound_syn(&buf) -> Option<u16>                     # 新增
↓
if Some(port) = ... {
    ensure_listeners_for_port(port, &mut sockets, ...)       # 新增（幂等）
}
↓
iface.poll(...)                                              # 同一帧 smoltcp 即可 accept
↓
flush_tx + 遍历 registry.all_handles() 处理首包                # 改成遍历全 registry
```

### SYN inspector 语义

```text
inspect_inbound_syn(packet: &[u8]) -> Option<u16>
```

- 解析 IPv4 + TCP（用 `etherparse::PacketHeaders::from_ip_slice`）。
- 仅当 IPv4 + TCP + `syn == true` + `ack == false` 时返回 `Some(dst_port)`。
- 其它情况（非 IPv4 / 非 TCP / 不是干净 SYN / 解析失败）返回 `None`。
- 纯函数，可单测；不修改 packet、不持有状态。

### Listener registry

```text
struct ListenerRegistry {
    // port -> 该端口上的 socket 槽位（Stage 8 的 pool 改成每端口一份）
    ports: HashMap<u16, Vec<SocketHandle>>,
    pool_size: usize,
}

impl ListenerRegistry {
    fn ensure_port(&mut self, port: u16, sockets: &mut SocketSet,
                   socket_ctxs: &mut HashMap<...>) -> Result<(), Capped>;
    fn all_handles(&self) -> impl Iterator<Item = SocketHandle>;
}
```

- `ensure_port` 幂等：端口已在册时直接返回 `Ok(())`。
- 当 `ports.len() >= MAX_INTERCEPTED_PORTS` 且端口不在册时返回 `Err(Capped)`，
  rx 路径日志告警，但**不要 panic**，**不要破坏现有端口**。

### SocketCtx 简化

`SocketCtx.local_port` 已经存在（Stage 8 保留用于 rearm），不需要新字段。

### 配置变化

| 配置 | 现在 | Stage 9 |
|---|---|---|
| `MINI_VPN_TUN_LOCAL_PORT` | 默认 80 | **移除** |
| `MINI_VPN_TUN_POOL_SIZE` | 默认 4 | 默认 **2**（per-port） |
| `MAX_INTERCEPTED_PORTS` | n/a | 编译期常量 **64** |
| 其余（`MINI_VPN_TUN_SERVER_ADDR` / `TLS_SNI` / `CA_PATH`） | 不变 | 不变 |

## 校验与失败语义

- SYN inspector 解析失败 → 当作 `None`（让 smoltcp 自己决定丢弃）。
- 端口数到顶 → 新端口日志告警，已注册端口不受影响。
- `ensure_port` 创建 socket 失败 → 同上，告警不 panic。
- 不在热路径里 `unwrap()`。

中文要点：SYN flood 必须**优雅退化**（拒绝新端口，旧端口继续服务），不能拖垮进程。

## 日志与可观测性

新增：
- `🆕 listener pool created for port {port} (pool_size={n})`：首次见到该端口的 SYN。
- `⚠️ intercepted port cap reached ({MAX}), drop SYN to port {port}`：到顶告警。

启动 log 去掉 `local_port=` 字段：

```text
🚀 TUN runtime started with pool_size={}, server_addr=..., tls_sni=..., ca_path=...
```

## 测试策略

### 单元测试

- `inspect_inbound_syn`:
  - 真 SYN（syn=1, ack=0）→ Some(dst_port)
  - SYN-ACK（syn=1, ack=1）→ None
  - 普通 ACK → None
  - 非 TCP（ICMP）→ None
  - 损坏包 → None
- `ListenerRegistry::ensure_port`:
  - 首次创建：`ports[port].len() == pool_size`
  - 重复调用：幂等（计数不变）
  - 到顶时返回 `Err(Capped)`，已注册端口不变
- 现有 Stage 8 测试更新到新 API（删去 `local_port` 配置入口，pool_size 默认 2）

### 本机手动联调（验收 recipe）

跨机拓扑（沿用 Stage 8）：

1. 同 Stage 8 准备(US server `47.251.188.205`，client env 指向它)。
2. 客户端启动，注意监听端口字段从 log 消失（因为已经没有"固定端口"概念）。
3. **端口 80 验证**：
   ```bash
   sudo route -n add -host 1.1.1.1 -interface "$UT"
   curl -v -m 10 http://1.1.1.1/
   sudo route -n delete -host 1.1.1.1
   ```
   预期：client 日志 `🆕 listener pool created for port 80` + `🎯 ... 1.1.1.1:80`，
   server 日志 `解析出的目标地址是: 1.1.1.1:80`，curl 收 301。
4. **端口 443 验证**（关键，证明任意端口）：
   ```bash
   sudo route -n add -host 1.1.1.1 -interface "$UT"
   curl -v -m 10 -k https://1.1.1.1/
   sudo route -n delete -host 1.1.1.1
   ```
   预期：client 日志 `🆕 listener pool created for port 443` + `🎯 ... 1.1.1.1:443`，
   curl 收到 Cloudflare HTTPS 响应（200 或重定向）。`-k` 是因为 IP 字面量与证书 SNI 不匹配。

## 文件范围

- `src/client_tun.rs`：删 `DEFAULT_TUN_LISTEN_PORT` / `MINI_VPN_TUN_LOCAL_PORT`；
  pool_size 默认从 4 改 2；新增 `inspect_inbound_syn` + `ListenerRegistry`；
  rx/timer 两个分支接线；测试更新。
- `docs/tech/2026-05-29-stage-9-syn-driven-dynamic-ports-plan.md`（任务清单）
- `docs/tech/09-syn-driven-dynamic-ports.md`（教学笔记）

## 验收标准

1. 不改源码、不改配置，client-tun 能拦截**任意目的端口**的 TCP 连接，
   首包到达时按 (dst_ip, dst_port) 提取出 Target。
2. 同时验证 80 + 443 两个端口（同跨机拓扑），curl 都收到 Upstream 出口返回的内容。
3. `cargo test` / `cargo check` / `cargo clippy --all-targets --all-features -- -D warnings`
   / `cargo doc --no-deps` 全过。
4. SYN flood 不 panic、不耗光内存（到 MAX_INTERCEPTED_PORTS 时优雅拒绝）。

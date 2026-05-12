# 06 TUN Upstream Minimal Config

## 背景

Stage 5 解决了本地监听面的 3 个硬编码：

- local port
- target address
- pool size

但 TUN 客户端在上游连接面仍然保留了 2 个硬编码：

- `127.0.0.1:8081`
- `localhost`

这意味着只要你想切换上游代理地址，或者 TLS 握手时的 SNI 不同，就必须重新改源码。

Stage 6 的目标，就是把这两个值也搬到启动配置里。

## 为什么要拆成 listener / upstream

如果继续把所有字段平铺在一个 `TunRuntimeConfig` 里，后面再加：

- `cert_path`
- reconnect policy
- upstream failover

结构会越来越混乱，因为“本地拦截面”和“远端外联面”其实是两类不同职责。

所以 Stage 6 把配置拆成：

- `TunListenerConfig`
- `TunUpstreamConfig`

中文要点：前者负责“本地截获什么”，后者负责“远端连谁、握手叫什么名字”。

## 新结构

```text
TunRuntimeConfig
├── listener: TunListenerConfig
│   ├── local_port
│   ├── target_addr
│   └── pool_size
└── upstream: TunUpstreamConfig
    ├── server_addr
    └── tls_sni
```

这样一来：

- listener 配置继续服务 TUN 监听池
- upstream 配置专门服务 `TcpStream + TLS + Yamux`

## 新增环境变量

Stage 6 新增：

```bash
MINI_VPN_TUN_SERVER_ADDR
MINI_VPN_TUN_TLS_SNI
```

默认值保持当前行为：

```text
server_addr = 127.0.0.1:8081
tls_sni = localhost
```

## 校验策略

这一步最重要的不是“能传值”，而是“坏值不能混进热路径”。

因此 Stage 6 的策略是：

- `server_addr` 在启动时就做格式校验
- `tls_sni` 在启动时就尝试 `ServerName::try_from(...)`

如果用户显式传了非法值，启动直接失败，不做静默回退。

中文要点：默认值只用于“没传”，不是用于“传错了帮你兜底”。

## 运行流变化

### Stage 5 之前

```text
start_tun_proxy()
-> read listener config
-> print local startup info
-> create TUN
-> hardcoded ServerName::try_from("localhost")
-> hardcoded TcpStream::connect("127.0.0.1:8081")
```

### Stage 6 之后

```text
start_tun_proxy()
-> TunRuntimeConfig::from_env()
-> derive listener config
-> read upstream config
-> print local + upstream startup info
-> create TUN
-> ServerName::try_from(upstream_tls_sni)
-> TcpStream::connect(upstream_server_addr)
```

## 使用示例

默认启动：

```bash
cargo run -- client-tun
```

覆盖 upstream 参数：

```bash
MINI_VPN_TUN_SERVER_ADDR=127.0.0.1:9000 \
MINI_VPN_TUN_TLS_SNI=example.com \
cargo run -- client-tun
```

如果配置生效，启动日志会显示类似内容：

```text
TUN runtime started with local_port=80, pool_size=4, target=httpbin.org:80, server_addr=127.0.0.1:9000, tls_sni=example.com
```

## 测试策略

本阶段重点测试 4 类情况：

- 默认 upstream 值是否正确
- listener + upstream 混合覆盖是否正确
- 非法 `server_addr` 是否被拒绝
- 非法 `tls_sni` 是否被拒绝

这一步的核心不是测试网络能不能连通，而是测试“启动配置是不是稳定、显式、可预测”。

## 这一步的收益

- 不再需要为切换上游地址改源码
- 不再需要为切换 TLS SNI 改源码
- 启动日志能一次性看清本地监听面和上游外联面
- 为下一阶段引入 `cert_path` 打好了边界

## 下一步

Stage 6 之后，一个自然的下一步是：

- add `cert_path`
- decide whether to share upstream config with `client-direct`

但这应该建立在 Stage 6 先稳定通过之后。

中文要点：先把最小外联配置版跑稳，再扩证书路径和统一客户端配置层，不要一次把多个风险点绑在一起。

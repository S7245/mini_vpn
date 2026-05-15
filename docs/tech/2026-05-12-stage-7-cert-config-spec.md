# 2026-05-12 Stage 7 Cert Config Spec

## 背景

Stage 6 已经把 TUN 客户端的上游连接面拆成了：

- `server_addr`
- `tls_sni`

并且已经验证：

- 默认链路可通
- `server_addr` 覆盖可通
- `tls_sni` 覆盖可生效

但当 `tls_sni=example.com` 时，TLS 握手失败，根因不是地址配置失败，而是当前证书材料仍然硬编码：

- client 侧固定读取 `cert.pem` 作为信任根
- server 侧固定读取 `cert.pem/key.pem` 作为服务端证书和私钥

这意味着“连谁”和“信谁”已经可配置，但“拿哪套证书材料”仍不可配置。

中文要点：Stage 7 不是再做一层网络逻辑，而是把 TLS 证书材料也纳入运行时配置边界，让地址、SNI、证书三者真正对齐。

## 目标

Stage 7 的最小目标：

1. 服务端支持配置：
   - `bind_addr`
   - `cert_path`
   - `key_path`
2. TUN 客户端支持配置：
   - `server_addr`
   - `tls_sni`
   - `ca_path`
3. 默认值保持兼容：
   - server cert path = `cert.pem`
   - server key path = `key.pem`
   - client ca path = `cert.pem`
4. 提供一套本地测试证书脚本，支持 SAN 至少覆盖：
   - `localhost`
   - `example.com`
   - 可选 `127.0.0.1`

## 非目标

本阶段明确不做：

- 多证书按 SNI 动态切换
- 证书热更新 / 热轮转
- 生产级 CA 生命周期管理
- client-direct 与 client-tun 的 TLS 配置统一
- 双向 TLS / 客户端证书认证
- 自动化 ACME / Let's Encrypt 集成

中文要点：先把“单套证书材料可切换、SNI 可对齐、本地联调可复现”跑稳，不提前引入生产复杂度。

## 架构边界

### Server 侧

继续保留 `ServerRuntimeConfig` 负责监听地址，并新增 `ServerTlsConfig` 负责 TLS 材料路径：

```text
ServerRuntimeConfig
└── bind_addr

ServerTlsConfig
├── cert_path
└── key_path
```

职责边界：

- `ServerRuntimeConfig` 只描述“监听在哪”
- `ServerTlsConfig` 只描述“拿哪张证书和哪把私钥”

### Client TUN 侧

继续保留现有：

- `TunListenerConfig`
- `TunUpstreamConfig`

并新增 `TunTlsConfig`：

```text
TunRuntimeConfig
├── listener: TunListenerConfig
├── upstream: TunUpstreamConfig
└── tls: TunTlsConfig
```

其中：

- `listener` 负责本地拦截面
- `upstream` 负责远端地址与 SNI
- `tls` 负责 CA 文件路径

## 启动流

### Server

```text
run()
-> ServerRuntimeConfig::from_env()
-> ServerTlsConfig::from_env()
-> load cert_path/key_path
-> build rustls ServerConfig
-> bind TcpListener(bind_addr)
-> accept TCP
-> TLS handshake
-> Yamux
```

### Client TUN

```text
start_tun_proxy()
-> TunRuntimeConfig::from_env()
-> TunTlsConfig::from_env()
-> load ca_path
-> build rustls ClientConfig
-> validate tls_sni
-> TcpStream::connect(server_addr)
-> TLS handshake with tls_sni
-> Yamux
```

中文要点：文件路径错误必须在启动阶段暴露，不允许拖到热路径里才炸。

## 配置项

### Server 环境变量

```text
MINI_VPN_SERVER_BIND_ADDR
MINI_VPN_SERVER_CERT_PATH
MINI_VPN_SERVER_KEY_PATH
```

默认值：

```text
bind_addr = 127.0.0.1:8081
cert_path = cert.pem
key_path = key.pem
```

### Client TUN 环境变量

```text
MINI_VPN_TUN_SERVER_ADDR
MINI_VPN_TUN_TLS_SNI
MINI_VPN_TUN_CA_PATH
```

默认值：

```text
server_addr = 127.0.0.1:8081
tls_sni = localhost
ca_path = cert.pem
```

## 校验策略

### 启动期校验

启动时必须完成：

- 地址格式校验
- `tls_sni` 格式校验
- 证书文件路径存在性校验
- 证书 PEM 解析校验
- 私钥 PEM/PKCS8 解析校验

### 失败语义

坏输入必须直接失败，且日志可分辨：

- 地址错误
- 文件不存在
- PEM 解析失败
- 私钥格式错误
- 证书名与 `tls_sni` 不匹配

中文要点：默认值只在“没传”时兜底，绝不对“传错了”做静默修复。

## 本地证书脚本

新增一个最小脚本，例如：

```text
scripts/gen-test-certs.sh
```

职责：

- 生成自签名测试证书或本地测试 CA
- 生成包含 SAN 的服务端证书
- 第一版至少覆盖：
  - `DNS:localhost`
  - `DNS:example.com`
  - `IP:127.0.0.1`

脚本输出建议：

- `certs/dev/server-cert.pem`
- `certs/dev/server-key.pem`
- `certs/dev/ca-cert.pem`

中文要点：不要继续把测试证书和根证书概念混在一张现成 `cert.pem` 里，让材料边界更清楚。

## 日志与可观测性

启动日志建议补充：

### Server

```text
server bind addr
server cert path
server key path
```

### Client TUN

```text
server_addr
tls_sni
ca_path
```

目标是让用户看到日志就能快速判断：

- 地址有没有生效
- SNI 有没有生效
- 当前到底加载了哪套证书材料

## 测试策略

### 单元测试

- `ServerTlsConfig` 默认值正确
- `ServerTlsConfig` 非法路径被拒绝
- `TunTlsConfig` 默认值正确
- `TunTlsConfig` 非法路径被拒绝
- 现有 `tls_sni` 非法值测试继续保留

### 本机联调

1. 默认证书 + 默认地址
2. `9000` 地址覆盖 + `localhost`
3. `9000` 地址覆盖 + `example.com`
4. 切错 `ca_path` 时启动或握手失败

## 文件范围

预计涉及：

- `src/server.rs`
- `src/client_tun.rs`
- `docs/tech/2026-05-12-stage-7-cert-config-plan.md`
- `docs/tech/07-cert-path-and-sni-alignment.md`
- `scripts/gen-test-certs.sh`

## 验收标准

Stage 7 完成时，应满足：

1. 不改源码，只改环境变量和证书文件路径，就能切换不同证书材料
2. 默认链路继续保持通过
3. `server_addr=127.0.0.1:9000 + tls_sni=example.com` 在证书 SAN 匹配时握手成功
4. `cargo test`
5. `cargo check`
6. `cargo clippy --all-targets --all-features -- -D warnings`
7. `cargo doc --no-deps`

全部通过。

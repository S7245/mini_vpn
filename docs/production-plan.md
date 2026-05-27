# 生产级 VPN 系统方案规划

## Context

mini_vpn 当前已完成数据面原型：TLS + Yamux 隧道、TCP/UDP 中继、TUN 虚拟网卡模式（smoltcp 用户态协议栈）、4 槽位监听池。

用户需求升级：深圳用户通过多台美国/英国中继节点访问目标（TCP + UDP），支持 iOS / Android / macOS / Windows 客户端，高并发，并有 PostgreSQL 支撑的控制面。

本方案将现有 Rust 代码演进为三层生产架构：数据面（中继节点）+ 控制面（API + PostgreSQL）+ 客户端（各平台 App）。

---

## 1. 整体架构分层

```
┌─────────────────────────────────────────────────────────────┐
│  Client Apps (iOS / Android / macOS / Windows)              │
│  Rust Core Library (UniFFI) + Platform VPN Extension        │
└──────────────────────────┬──────────────────────────────────┘
                           │ TLS 443 (伪装 HTTPS)
                    ┌──────▼──────────────┐
                    │  Relay Servers       │
                    │  US-1…US-N (美国)    │
                    │  UK-1…UK-N (英国)    │
                    │  (mini_vpn server)   │
                    └──────────┬──────────┘
                               │ TCP/UDP → Internet
                    ┌──────────▼──────────────┐
                    │  Control Plane API       │
                    │  (Rust Axum)             │
                    │  + PostgreSQL            │
                    └─────────────────────────┘
```

**三大组件**：
- **数据面**：中继节点（现有 `server.rs` 演进）— 只负责转流量
- **控制面**：API 服务 + PostgreSQL — 用户管理、节点调度、鉴权
- **客户端**：Rust 核心库 + 各平台壳 — TUN/VPN 模式拦截全局流量

---

## 2. 数据面方案（演进路径）

### 2.1 协议层

| 层 | 当前 | 生产演进 |
|---|---|---|
| 传输加密 | TLS 1.3 (rustls) | 保留，端口改为 **443** |
| 多路复用 | yamux | 保留，每连接 N 个并发子流 |
| 握手伪装 | fake HTTP GET header | 保留，考虑升级为 HTTPS 完整握手 |
| 认证 | 无 | **在伪装头后追加 Token 行**（最小侵入） |
| 协议编码 | 文本（`TCP host:port`） | 先保留文本，高并发压测后再考虑二进制 |

**认证升级方案**（最小改动，改 `shared/relay_protocol.rs`）：
```
GET / HTTP/1.1\r\nHost: www.bing.com\r\n\r\n   ← 伪装头（不变）
TCP www.google.com:443\n                        ← 中继请求（不变）
AUTH <jwt_token>\n                              ← 新增认证行
```
- `write_relay_request()` / `read_relay_request()` 在 `shared/relay_protocol.rs` 中统一扩展
- Server 端验证 JWT 签名，失败直接关闭子流，不影响其他 Yamux 子流

### 2.2 高并发设计

```
Client → 1条 TLS 长连接 → Yamux（支持 1000+ 并发子流）
                              ├─ substream-1: TCP to google.com
                              ├─ substream-2: UDP to 8.8.8.8
                              └─ substream-N: ...
```

- 单 Yamux 连接可承载数百并发流，**不需要连接池**（Yamux 本身就是多路复用）
- 服务端每个 TLS 连接 → 独立 Yamux `Connection` → 独立 `tokio::spawn` 处理子流
- 每个子流独立 `tokio::spawn` 双向中继，互不干扰（`server.rs:168-343` 已实现）

**服务端并发能力估算**：
- 1 台 4 核服务器：~10,000 并发 Yamux 子流（以 64KB buffer/流，内存约 640MB）
- N 台服务器通过 GeoDNS 分流

### 2.3 多服务器 & 故障转移

客户端侧（`client_tun.rs` / `client.rs` 演进）：

```rust
struct UpstreamPool {
    servers: Vec<ServerEndpoint>,  // 从控制面 API 拉取，按延迟排序
    active: usize,                  // 当前使用的服务器索引
}
// 当前连接断开 → 指数退避重试（2s→4s→8s→16s）→ 切换下一节点
```

- 启动时从控制面 API 拉取节点列表（按延迟排序）
- 当前 `TLS + Yamux` 连接断开 → 自动尝试下一个节点
- 对应 `TODO.md` 中 "Add reconnect policy" 和 "Add upstream failover"

### 2.4 中继节点部署

```
GeoDNS / DNS 轮询
  ├─ relay-us-1.example.com → IP1
  ├─ relay-us-2.example.com → IP2
  └─ relay-uk-1.example.com → IP3

每台服务器：
  systemd service → cargo run -- server
  监听端口: 443 (需 root 或 CAP_NET_BIND_SERVICE)
  TLS 证书: Let's Encrypt (SNI: relay-us-1.example.com)
```

- 无需前置 LB（TLS 在 Rust 进程内终止，减少延迟）
- Nginx 可选用于端口复用（443 → TLS SNI 路由 → mini_vpn 进程）

---

## 3. 控制面方案（新增组件）

### 3.1 技术选型

**推荐：Rust + Axum + SQLx + PostgreSQL**

理由：
- 与数据面同语言，共享认证 crate（如 `jsonwebtoken`）
- Axum 性能优秀，适合高并发 API
- SQLx 编译期 SQL 校验，类型安全

控制面职责：
- 用户注册/登录/鉴权（JWT 签发）
- 服务器节点注册与心跳上报
- 客户端拉取节点列表（按延迟、负载排序）
- 用量统计（流量 / 连接数）
- 订阅管理（免费/付费额度）

### 3.2 PostgreSQL 数据模型

```sql
-- 用户表
CREATE TABLE users (
    id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    email         TEXT UNIQUE NOT NULL,
    password_hash TEXT NOT NULL,
    plan          TEXT NOT NULL DEFAULT 'free',   -- free/pro/enterprise
    quota_bytes   BIGINT DEFAULT 10737418240,      -- 10GB
    used_bytes    BIGINT DEFAULT 0,
    created_at    TIMESTAMPTZ DEFAULT now(),
    expires_at    TIMESTAMPTZ
);

-- 中继节点表
CREATE TABLE relay_nodes (
    id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    name       TEXT NOT NULL,               -- "US-West-1"
    host       TEXT NOT NULL,               -- relay-us-1.example.com
    port       INT NOT NULL DEFAULT 443,
    region     TEXT NOT NULL,               -- "us-west" / "uk-london"
    is_active  BOOL DEFAULT true,
    load_pct   SMALLINT DEFAULT 0,          -- 0-100，由节点心跳更新
    latency_ms INT,                         -- 由控制面 ping 测量
    updated_at TIMESTAMPTZ DEFAULT now()
);

-- 会话表（连接记录）
CREATE TABLE sessions (
    id         UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id    UUID REFERENCES users(id),
    node_id    UUID REFERENCES relay_nodes(id),
    started_at TIMESTAMPTZ DEFAULT now(),
    ended_at   TIMESTAMPTZ,
    bytes_up   BIGINT DEFAULT 0,
    bytes_down BIGINT DEFAULT 0,
    client_ip  INET,
    platform   TEXT    -- ios/android/macos/windows
);

-- JWT 吊销黑名单（登出/强制下线）
CREATE TABLE revoked_tokens (
    jti        TEXT PRIMARY KEY,
    expires_at TIMESTAMPTZ NOT NULL
);

-- 索引
CREATE INDEX ON sessions (user_id, started_at DESC);
CREATE INDEX ON relay_nodes (region, is_active, load_pct);
```

### 3.3 核心 API

```
POST /api/auth/register        # 注册
POST /api/auth/login           # 登录 → 返回 JWT
GET  /api/nodes                # 获取节点列表（按延迟排序）
POST /api/nodes/:id/heartbeat  # 节点心跳上报（负载 + 连接数）
GET  /api/user/usage           # 查询用量
POST /api/sessions             # 连接建立时上报（可选，用于统计）
```

### 3.4 节点认证

中继节点向控制面上报心跳时使用**节点密钥**（独立于用户 JWT），写入各服务器环境变量。

---

## 4. 客户端架构

### 4.1 Rust 核心库（跨平台 UniFFI）

将 `client_tun.rs` + `shared/` 抽取为独立 crate `mini_vpn_core`：

```
mini_vpn_core/
├── src/
│   ├── lib.rs          # UniFFI 导出接口
│   ├── engine.rs       # VPN 引擎（start/stop/status）
│   ├── tunnel.rs       # TLS + Yamux 连接管理 + 重连
│   ├── tun_gateway.rs  # TUN 模式（smoltcp）
│   ├── direct_proxy.rs # SOCKS5 直连路径
│   └── config.rs       # VpnConfig（节点列表、token）
├── mini_vpn_core.udl   # UniFFI 接口定义
```

UniFFI 接口定义（`mini_vpn_core.udl`）：

```
interface VpnEngine {
    constructor(VpnConfig config);
    [Throws=VpnError]
    void start();
    void stop();
    VpnStatus status();
};
```

编译目标：
- iOS: `aarch64-apple-ios`
- Android: `aarch64-linux-android` / `armv7-linux-androideabi`
- macOS: `aarch64-apple-darwin` / `x86_64-apple-darwin`
- Windows: `x86_64-pc-windows-msvc`

### 4.2 各平台壳接入方式

| 平台 | VPN 接入点 | 语言 | 备注 |
|---|---|---|---|
| iOS | NEPacketTunnelProvider | Swift | 需要 Network Extension 权限 |
| Android | VpnService | Kotlin | 系统 VpnService.Builder |
| macOS | Network Extension / utun | Swift | 需要签名 entitlement |
| Windows | WinTun.dll 驱动 | Rust 直接调用 | 需要管理员权限 |

**iOS/macOS 关键适配点**：
- `NEPacketTunnelProvider.startTunnel()` → 调用 Rust `VpnEngine.start()`
- `NEPacketTunnelProvider.handleAppMessage()` → 控制状态查询
- 数据包路由：`NEIPv4Settings` 设置路由表 → 系统将流量送入 Tunnel Extension → Rust 读取处理

**Android 关键适配点**：
- `VpnService.Builder.establish()` → 获取 fd → 传给 Rust（`tun` crate 支持从 fd 创建）
- JNI 或 UniFFI Kotlin 绑定调用 `VpnEngine`

---

## 5. 流量伪装策略（应对 GFW）

| 优先级 | 策略 | 变更内容 |
|---|---|---|
| **P0** | 端口改 443 | `MINI_VPN_SERVER_BIND_ADDR=0.0.0.0:443` |
| **P0** | SNI 改正规域名 | `relay-us-1.example.com`（含 Let's Encrypt 证书）|
| P1 | 握手完整化 | TLS + 返回 HTTP 200 页面（欺骗探测器）|
| P2 | 流量噪点 | Yamux 帧随机 padding（低优先级）|
| P3 | CDN 前置 | Cloudflare Workers 中转（高匿名，但增加延迟）|

**最高优先级改动（P0）**：
1. 服务端监听 **443**，TLS 证书 SNI 配置为正规域名
2. 客户端连接时使用正规域名作为 SNI：`ServerName::try_from("relay-us-1.example.com")`

---

## 6. 分阶段交付计划

### Phase 1：数据面加固（2-3 周）

目标：现有 mini_vpn 具备生产可用的可靠性

| 任务 | 涉及文件 | 工作量 |
|---|---|---|
| 断线自动重连（指数退避） | `client_tun.rs`, `client.rs` | M |
| 多节点故障转移（`UpstreamPool`） | `client_tun.rs` | M |
| 协议添加 Token 认证行 | `shared/relay_protocol.rs`, `server.rs` | S |
| 移除热路径 `unwrap()` | 全局 | M |
| 443 端口 + 正规 SNI | 配置默认值 | S |
| UDP over TUN 对齐 | `client_tun.rs` | L |

验收：`cargo test` 全通过；3 个节点可切换；重启服务端后客户端自动恢复

---

### Phase 2：控制面 MVP（3-4 周）

目标：用户可注册、获取节点列表、服务器有心跳管理

| 任务 | 说明 |
|---|---|
| 新建 `control_api/` crate | Axum + SQLx + PostgreSQL |
| 用户注册/登录 API | 返回 JWT |
| 节点注册与心跳 API | 节点上报负载、延迟 |
| `GET /api/nodes` | 按延迟排序，客户端拉取 |
| 客户端启动时动态拉取节点列表 | `config.rs` 中实现 HTTP 请求 |
| PostgreSQL 初始化脚本 | `migrations/001_init.sql` |

验收：`curl /api/nodes` 返回节点列表；客户端连接时验证 JWT

---

### Phase 3：Rust 核心库 + UniFFI（3-4 周）

目标：核心逻辑封装为跨平台库

| 任务 | 说明 |
|---|---|
| 抽取 `mini_vpn_core` crate | 与 bin 分离 |
| 编写 UniFFI `.udl` 接口定义 | `start/stop/status/config` |
| 编译 iOS / Android 静态库 | `cargo build --target` |
| macOS 框架打包（`.xcframework`） | `lipo` + `xcodebuild` |
| Windows `.dll` 构建 | CI 交叉编译 |

验收：`uniffi-bindgen` 生成 Swift / Kotlin 绑定；单元测试在所有目标通过

---

### Phase 4：平台客户端壳（4-6 周）

目标：真实 App 在 4 个平台可用

| 平台 | 主要工作 |
|---|---|
| **macOS** | Swift Network Extension + 链接 Rust xcframework |
| **iOS** | 与 macOS 共享 Extension 代码，差异在证书和路由配置 |
| **Android** | Kotlin VpnService + JNI/UniFFI 调用 Rust |
| **Windows** | Rust + `wintun` crate 直接实现，无需额外壳语言 |

验收：4 平台可连接节点、访问 ipinfo.io 返回美国/英国 IP

---

### Phase 5：监控与生产加固（持续）

| 任务 | 工具 |
|---|---|
| 中继节点 metrics 上报 | Prometheus + Grafana |
| 结构化日志 | `tracing` + `tracing-subscriber`（JSON 格式）|
| 自动证书续签 | certbot + systemd timer |
| 并发压测 | k6 + `cargo bench` |
| 用量限速 | 控制面令牌桶 + 节点侧限速 |

---

## 7. 关键文件变更清单

| 文件 | 变更性质 |
|---|---|
| `src/shared/relay_protocol.rs` | 添加 Token 认证行的读写 |
| `src/server.rs` | JWT 验证、443 端口默认值 |
| `src/client_tun.rs` | UpstreamPool 重连、断线重试 |
| `src/client.rs` | 同上 |
| `Cargo.toml` | 添加 `jsonwebtoken`, `axum`, `sqlx` 依赖 |
| `control_api/`（新建） | Axum API 服务 |
| `mini_vpn_core/`（抽取） | UniFFI 跨平台核心库 |
| `migrations/001_init.sql`（新建） | PostgreSQL schema |

---

## 8. 验收测试方案

```bash
# 数据面验收
cargo test --workspace

# 启动服务端（443 端口）
MINI_VPN_SERVER_BIND_ADDR=0.0.0.0:443 cargo run -- server

# 客户端连接并访问美国 IP
curl --socks5 127.0.0.1:1080 ipinfo.io  # 应返回美国 IP

# 控制面验收
curl -X POST /api/auth/login -d '{"email":"test@test.com","password":"xxx"}'
curl -H "Authorization: Bearer <token>" /api/nodes  # 应返回节点列表

# 多节点故障转移验收
# 关闭当前连接节点 → 客户端应在 5s 内自动切换到备用节点

# 并发压测
# 1000 并发 curl 通过 SOCKS5 代理 → 观察服务端 Yamux 子流数量和 P99 延迟
```

---

## 9. 关键取舍说明

| 决策 | 选择 | 理由 |
|---|---|---|
| mTLS vs Token 认证 | **JWT Token** | mTLS 需为每用户签发证书，运维成本高；JWT 更灵活、易吊销 |
| 连接池 vs 单 Yamux | **单连接 Yamux 多子流** | Yamux 天然支持 1000+ 并发流，无需额外连接池 |
| 控制面语言 | **Rust Axum** | 与数据面共享 crate，减少语言边界；团队 Go 更熟可换 |
| 文本 vs 二进制协议 | **保留文本，暂不升级** | 瓶颈在网络 RTT，过早优化增加复杂度 |
| CDN 前置 vs 直连 | **Phase 1-4 直连** | CDN 前置增加延迟，先验证直连稳定性 |

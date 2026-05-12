# Stage 6 TUN Upstream Minimal Configuration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add minimal upstream runtime configuration for the TUN client so `server_addr` and `tls_sni` become startup-configurable while preserving current defaults.

**Architecture:** Split the current flat TUN config into local listener config and upstream tunnel config. Keep the change isolated to `src/client_tun.rs`, validate upstream values at startup, and leave the listener pool and relay protocol unchanged.

**Tech Stack:** Rust, Tokio, tokio-rustls, smoltcp, tun, yamux, cargo test/check/clippy/doc, git

---

## File Map

- Modify: `src/client_tun.rs`
  - Split `TunRuntimeConfig`
  - Add `TunListenerConfig`
  - Add `TunUpstreamConfig`
  - Replace hardcoded upstream address and SNI
  - Add English-led + Chinese-key-point comments
  - Add upstream config unit tests
- Create: `docs/tech/06-tun-upstream-minimal-config.md`
  - Stage 6 teaching note

## Task 1: Split Runtime Config Into Listener And Upstream Parts

**Files:**
- Modify: `src/client_tun.rs`
- Test: `src/client_tun.rs`

- [ ] **Step 1: Write the failing default-upstream test**

Add this unit test in the existing `#[cfg(test)]` module in `src/client_tun.rs`:

```rust
#[test]
fn tun_runtime_config_defaults_include_upstream_values() {
    let config = TunRuntimeConfig::from_sources(None, None, None, None, None)
        .expect("config should load");

    assert_eq!(config.listener.local_port, 80);
    assert_eq!(config.listener.pool_size, 4);
    assert_eq!(config.listener.target_addr.to_wire_string(), "httpbin.org:80");
    assert_eq!(config.upstream.server_addr, "127.0.0.1:8081");
    assert_eq!(config.upstream.tls_sni, "localhost");
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run:

```bash
cargo test tun_runtime_config_defaults_include_upstream_values -- --nocapture
```

Expected:

```text
error[E0061]: this function takes 3 arguments but 5 arguments were supplied
```

- [ ] **Step 3: Refactor the config types**

In `src/client_tun.rs`, replace the current flat `TunRuntimeConfig` with:

```rust
const DEFAULT_TUN_SERVER_ADDR: &str = "127.0.0.1:8081";
const DEFAULT_TUN_TLS_SNI: &str = "localhost";

/// Local listener-side startup configuration for the TUN runtime.
/// 中文要点：这一层只关心本地拦截面，不关心怎么连上游 TLS/Yamux 服务。
#[derive(Debug, Clone)]
struct TunListenerConfig {
    /// Local TCP port intercepted by the TUN-side smoltcp stack.
    /// 中文要点：虚拟网卡这一侧实际监听的本地端口。
    local_port: u16,
    /// Default remote relay target for the current TCP-over-TUN demo path.
    /// 中文要点：当前 TUN demo 默认转发到的远端目标。
    target_addr: TargetAddr,
    /// Number of listener slots created for the same local port.
    /// 中文要点：监听池槽位数，决定会创建多少个独立的监听房间。
    pool_size: usize,
}

impl TunListenerConfig {
    /// Build listener config from optional string sources.
    /// 中文要点：本地监听配置与上游外联配置分开解析，避免职责混淆。
    fn from_sources(
        local_port: Option<&str>,
        target_addr: Option<&str>,
        pool_size: Option<&str>,
    ) -> Result<Self, ClientError> {
        let local_port = match local_port {
            Some(value) => value
                .parse::<u16>()
                .map_err(|_| ClientError::InvalidTarget(format!("invalid local port: {value}")))?,
            None => DEFAULT_TUN_LISTEN_PORT,
        };

        let target_addr = match target_addr {
            Some(value) => TargetAddr::parse(value)?,
            None => TargetAddr::parse(DEFAULT_TUN_TARGET)?,
        };

        let pool_size = match pool_size {
            Some(value) => value
                .parse::<usize>()
                .map_err(|_| ClientError::InvalidTarget(format!("invalid pool size: {value}")))?,
            None => DEFAULT_TUN_POOL_SIZE,
        };

        if pool_size == 0 {
            return Err(ClientError::InvalidTarget(
                "invalid pool size: must be at least 1".to_string(),
            ));
        }

        Ok(Self {
            local_port,
            target_addr,
            pool_size,
        })
    }

    /// Derive the listener-pool blueprint from startup config.
    /// 中文要点：监听池蓝图依然只从 listener 配置派生，不受 upstream 字段影响。
    fn listener_spec(&self) -> ListenerSpec {
        ListenerSpec {
            local_port: self.local_port,
            pool_size: self.pool_size,
        }
    }
}

/// Upstream TLS/Yamux connection configuration for the TUN runtime.
/// 中文要点：这一层只描述“连谁”和“用什么 SNI”，不参与本地监听池逻辑。
#[derive(Debug, Clone)]
struct TunUpstreamConfig {
    /// TCP address of the upstream proxy server.
    /// 中文要点：TUN 客户端实际要连的上游服务地址。
    server_addr: String,
    /// TLS SNI value used during the upstream handshake.
    /// 中文要点：TLS 握手时发送给服务端的 Server Name。
    tls_sni: String,
}

impl TunUpstreamConfig {
    /// Build upstream config from optional string sources.
    /// 中文要点：外联配置在启动时完成校验，避免把坏值带到 TLS 热路径。
    fn from_sources(
        server_addr: Option<&str>,
        tls_sni: Option<&str>,
    ) -> Result<Self, ClientError> {
        let server_addr = server_addr.unwrap_or(DEFAULT_TUN_SERVER_ADDR).to_string();
        let tls_sni = tls_sni.unwrap_or(DEFAULT_TUN_TLS_SNI).to_string();

        server_addr
            .parse::<std::net::SocketAddr>()
            .map_err(|_| ClientError::InvalidTarget(format!("invalid upstream server addr: {server_addr}")))?;

        ServerName::try_from(tls_sni.as_str())
            .map_err(|_| ClientError::InvalidTarget(format!("invalid upstream tls sni: {tls_sni}")))?;

        Ok(Self {
            server_addr,
            tls_sni,
        })
    }
}

/// Startup configuration for the TUN runtime.
/// 中文要点：总配置壳只负责把 listener 与 upstream 两类配置组合起来。
#[derive(Debug, Clone)]
struct TunRuntimeConfig {
    listener: TunListenerConfig,
    upstream: TunUpstreamConfig,
}

impl TunRuntimeConfig {
    /// Build config from optional string sources.
    /// 中文要点：测试和环境变量入口共享同一套组合逻辑，避免行为漂移。
    fn from_sources(
        local_port: Option<&str>,
        target_addr: Option<&str>,
        pool_size: Option<&str>,
        server_addr: Option<&str>,
        tls_sni: Option<&str>,
    ) -> Result<Self, ClientError> {
        Ok(Self {
            listener: TunListenerConfig::from_sources(local_port, target_addr, pool_size)?,
            upstream: TunUpstreamConfig::from_sources(server_addr, tls_sni)?,
        })
    }
}
```

- [ ] **Step 4: Run the default-upstream test to verify it passes**

Run:

```bash
cargo test tun_runtime_config_defaults_include_upstream_values -- --nocapture
```

Expected:

```text
test client_tun::tests::tun_runtime_config_defaults_include_upstream_values ... ok
```

- [ ] **Step 5: Commit the config-split milestone**

Run:

```bash
git add src/client_tun.rs
git commit -m "refactor(tun): split listener and upstream config"
```

Expected:

```text
[main ...] refactor(tun): split listener and upstream config
```

## Task 2: Wire Environment Parsing And Startup Usage

**Files:**
- Modify: `src/client_tun.rs`
- Test: `src/client_tun.rs`

- [ ] **Step 1: Add a passing listener/upstream derivation test**

Add this test:

```rust
#[test]
fn tun_runtime_config_accepts_listener_and_upstream_overrides() {
    let config = TunRuntimeConfig::from_sources(
        Some("8080"),
        Some("127.0.0.1:7897"),
        Some("2"),
        Some("127.0.0.1:9000"),
        Some("example.com"),
    )
    .expect("config should load");

    let listener_spec = config.listener.listener_spec();

    assert_eq!(listener_spec.local_port, 8080);
    assert_eq!(listener_spec.pool_size, 2);
    assert_eq!(config.listener.target_addr.to_wire_string(), "127.0.0.1:7897");
    assert_eq!(config.upstream.server_addr, "127.0.0.1:9000");
    assert_eq!(config.upstream.tls_sni, "example.com");
}
```

- [ ] **Step 2: Run the derivation test to verify it passes**

Run:

```bash
cargo test tun_runtime_config_accepts_listener_and_upstream_overrides -- --nocapture
```

Expected:

```text
test client_tun::tests::tun_runtime_config_accepts_listener_and_upstream_overrides ... ok
```

- [ ] **Step 3: Update `from_env()` and `start_tun_proxy()`**

In `src/client_tun.rs`, add this `from_env()` implementation:

```rust
impl TunRuntimeConfig {
    /// Read config from process environment.
    /// 中文要点：Stage 6 在 Stage 5 基础上新增 upstream 配置入口，但仍保持最小环境变量方案。
    fn from_env() -> Result<Self, ClientError> {
        let local_port = std::env::var("MINI_VPN_TUN_LOCAL_PORT").ok();
        let target_addr = std::env::var("MINI_VPN_TUN_TARGET_ADDR").ok();
        let pool_size = std::env::var("MINI_VPN_TUN_POOL_SIZE").ok();
        let server_addr = std::env::var("MINI_VPN_TUN_SERVER_ADDR").ok();
        let tls_sni = std::env::var("MINI_VPN_TUN_TLS_SNI").ok();

        Self::from_sources(
            local_port.as_deref(),
            target_addr.as_deref(),
            pool_size.as_deref(),
            server_addr.as_deref(),
            tls_sni.as_deref(),
        )
    }
}
```

Then update `start_tun_proxy()` so these lines:

```rust
let listener_spec = runtime_config.listener_spec();
let default_target = runtime_config.target_addr.clone();
```

become:

```rust
let listener_spec = runtime_config.listener.listener_spec();
let default_target = runtime_config.listener.target_addr.clone();
let upstream_server_addr = runtime_config.upstream.server_addr.clone();
let upstream_tls_sni = runtime_config.upstream.tls_sni.clone();
```

Replace the current hardcoded startup log with:

```rust
println!(
    "🚀 TUN runtime started with local_port={}, pool_size={}, target={}, server_addr={}, tls_sni={}",
    listener_spec.local_port,
    listener_spec.pool_size,
    default_target.to_wire_string(),
    upstream_server_addr,
    upstream_tls_sni
);
```

Replace:

```rust
let domain = match ServerName::try_from("localhost") {
```

with:

```rust
let domain = match ServerName::try_from(upstream_tls_sni.as_str()) {
```

Replace:

```rust
let server_stream = TcpStream::connect("127.0.0.1:8081")
```

with:

```rust
let server_stream = TcpStream::connect(upstream_server_addr.as_str())
```

- [ ] **Step 4: Run `cargo check` to verify runtime wiring compiles**

Run:

```bash
cargo check
```

Expected:

```text
Finished `dev` profile ... target(s) in ...
```

- [ ] **Step 5: Commit the runtime-wiring milestone**

Run:

```bash
git add src/client_tun.rs
git commit -m "feat(tun): configure upstream server and tls sni"
```

Expected:

```text
[main ...] feat(tun): configure upstream server and tls sni
```

## Task 3: Add Upstream Validation Coverage

**Files:**
- Modify: `src/client_tun.rs`
- Test: `src/client_tun.rs`

- [ ] **Step 1: Add upstream validation tests**

Add these tests:

```rust
#[test]
fn tun_runtime_config_rejects_invalid_upstream_server_addr() {
    let err = TunRuntimeConfig::from_sources(None, None, None, Some("bad-addr"), None)
        .expect_err("invalid upstream server addr should fail");
    assert!(err
        .to_string()
        .contains("invalid upstream server addr"));
}

#[test]
fn tun_runtime_config_rejects_invalid_upstream_tls_sni() {
    let err = TunRuntimeConfig::from_sources(None, None, None, None, Some("bad sni"))
        .expect_err("invalid upstream tls sni should fail");
    assert!(err.to_string().contains("invalid upstream tls sni"));
}
```

- [ ] **Step 2: Run the targeted upstream tests**

Run:

```bash
cargo test upstream -- --nocapture
```

Expected:

```text
The upstream-related config tests pass.
```

- [ ] **Step 3: Re-run the full TUN config test subset**

Run:

```bash
cargo test tun_runtime_config_ -- --nocapture
```

Expected:

```text
All `tun_runtime_config_*` tests pass, including the new upstream validation cases.
```

- [ ] **Step 4: Commit the validation milestone**

Run:

```bash
git add src/client_tun.rs
git commit -m "test(tun): add upstream config validation coverage"
```

Expected:

```text
[main ...] test(tun): add upstream config validation coverage
```

## Task 4: Write The Stage 6 Teaching Note

**Files:**
- Create: `docs/tech/06-tun-upstream-minimal-config.md`

- [ ] **Step 1: Write the teaching note**

Create `docs/tech/06-tun-upstream-minimal-config.md` with content shaped like:

```md
# 06 TUN Upstream Minimal Config

## 背景

Stage 5 解决了本地监听面的 3 个硬编码：

- local port
- target address
- pool size

但 TUN 客户端在上游连接面仍然有 2 个硬编码：

- `127.0.0.1:8081`
- `localhost`

Stage 6 的目标，就是把这两个值也搬到启动配置里。

## 为什么拆成 listener / upstream

如果继续把所有字段平铺在一个 `TunRuntimeConfig` 里，后面再加：

- `cert_path`
- reconnect policy
- upstream failover

结构会越来越混乱。

所以 Stage 6 把配置拆成：

- `TunListenerConfig`
- `TunUpstreamConfig`

## 新增环境变量

```bash
MINI_VPN_TUN_SERVER_ADDR
MINI_VPN_TUN_TLS_SNI
```

## 示例

```bash
MINI_VPN_TUN_SERVER_ADDR=127.0.0.1:9000 \
MINI_VPN_TUN_TLS_SNI=example.com \
./target/debug/mini_vpn client-tun
```
```

- [ ] **Step 2: Run doc generation**

Run:

```bash
cargo doc --no-deps
```

Expected:

```text
Generated .../target/doc/mini_vpn/index.html
```

- [ ] **Step 3: Commit the Stage 6 docs**

Run:

```bash
git add docs/tech/06-tun-upstream-minimal-config.md
git commit -m "docs(tun): add stage 6 upstream config note"
```

Expected:

```text
[main ...] docs(tun): add stage 6 upstream config note
```

## Task 5: Full Validation And Runtime Smoke Check

**Files:**
- Modify: `src/client_tun.rs` (only if last-minute fixes are required)
- Modify: `docs/tech/06-tun-upstream-minimal-config.md` (only if commands need tuning)

- [ ] **Step 1: Run the full validation suite**

Run:

```bash
cargo test
cargo check
cargo clippy --all-targets --all-features -- -D warnings
cargo doc --no-deps
```

Expected:

```text
All commands succeed without warnings.
```

- [ ] **Step 2: Run a default startup smoke check**

Run:

```bash
cargo run -- client-tun
```

Expected:

```text
Startup log shows server_addr=127.0.0.1:8081 and tls_sni=localhost before any TUN permission boundary.
```

- [ ] **Step 3: Run an override startup smoke check**

Run:

```bash
MINI_VPN_TUN_SERVER_ADDR=127.0.0.1:9000 \
MINI_VPN_TUN_TLS_SNI=example.com \
cargo run -- client-tun
```

Expected:

```text
Startup log shows server_addr=127.0.0.1:9000 and tls_sni=example.com before any TUN permission boundary.
```

- [ ] **Step 4: Verify git status and recent history**

Run:

```bash
git status --short
git log --oneline -n 10
```

Expected:

```text
Working tree clean, and recent commits include the Stage 6 config milestones.
```

- [ ] **Step 5: If needed, create one final umbrella commit**

Run only if there are remaining uncommitted Stage 6 changes:

```bash
git add src/client_tun.rs docs/tech/06-tun-upstream-minimal-config.md
git commit -m "feat(tun): add minimal upstream runtime config"
```

Expected:

```text
[main ...] feat(tun): add minimal upstream runtime config
```

## Self-Review

### Spec Coverage

- split listener/upstream config: covered by Tasks 1 and 2
- add upstream env variables: covered by Task 2
- replace hardcoded upstream values: covered by Task 2
- validate upstream config: covered by Task 3
- Stage 6 teaching note: covered by Task 4
- full validation and smoke check: covered by Task 5

### Placeholder Scan

- No `TODO`, `TBD`, or vague deferred instructions remain
- Each task includes exact files, code, commands, and expected outputs

### Type Consistency

- `TunRuntimeConfig`, `TunListenerConfig`, `TunUpstreamConfig`, `from_sources()`, and `from_env()` are used consistently throughout the plan

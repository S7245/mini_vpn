# Stage 5 TUN Runtime Minimal Configuration Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use `superpowers:subagent-driven-development` (recommended) or `superpowers:executing-plans` to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a minimal `TunRuntimeConfig` so the TUN runtime can configure `local_port`, `target_addr`, and `pool_size` at startup while preserving current Stage 4 defaults.

**Architecture:** Keep the configuration layer local to `src/client_tun.rs` and separate configuration parsing from runtime assembly. Use one focused config struct that reads environment variables, validates them once during startup, derives `ListenerSpec`, and leaves the existing Stage 4 listener-pool runtime mostly unchanged.

**Tech Stack:** Rust, Tokio, smoltcp, tun, yamux, rustls, cargo test/check/clippy/doc, git

---

## File Map

- Modify: `src/client_tun.rs`
  - Add `TunRuntimeConfig`
  - Replace direct use of hardcoded runtime constants
  - Add English-led + Chinese-key-point comments
  - Add config parsing unit tests
- Create: `docs/tech/05-tun-runtime-minimal-config.md`
  - Stage 5 teaching note
- Keep: `docs/tech/2026-05-07-stage-4-acceptance-summary.md`
  - Already written as milestone A close-out; no edit expected unless wording needs correction

## Task 1: Add `TunRuntimeConfig` Defaults And Derivation

**Files:**
- Modify: `src/client_tun.rs`
- Test: `src/client_tun.rs`

- [ ] **Step 1: Write the failing default-config test**

Add this unit test in the existing `#[cfg(test)]` module in `src/client_tun.rs`:

```rust
#[test]
fn tun_runtime_config_defaults_match_stage4_behavior() {
    let config = TunRuntimeConfig::from_sources(None, None, None).expect("config should load");

    assert_eq!(config.local_port, 80);
    assert_eq!(config.pool_size, 4);
    assert_eq!(config.target_addr.to_wire_string(), "httpbin.org:80");
}
```

- [ ] **Step 2: Run the test to verify it fails**

Run:

```bash
cargo test tun_runtime_config_defaults_match_stage4_behavior -- --nocapture
```

Expected:

```text
error[E0433]: failed to resolve: use of undeclared type `TunRuntimeConfig`
```

- [ ] **Step 3: Add the `TunRuntimeConfig` type and helper methods**

Insert the following in `src/client_tun.rs` near the existing runtime types:

```rust
/// Startup configuration for the TUN runtime.
/// 中文要点：这是运行时配置入口，负责把环境变量转换成稳定、可校验的启动参数。
#[derive(Debug, Clone)]
struct TunRuntimeConfig {
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

impl TunRuntimeConfig {
    /// Build config from optional string sources.
    /// 中文要点：测试和环境变量入口共用同一套解析逻辑，避免两套规则漂移。
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

    /// Read config from process environment.
    /// 中文要点：Stage 5 只做最小配置版，因此先走环境变量入口，不引入额外 CLI 层。
    fn from_env() -> Result<Self, ClientError> {
        Self::from_sources(
            std::env::var("MINI_VPN_TUN_LOCAL_PORT").ok().as_deref(),
            std::env::var("MINI_VPN_TUN_TARGET_ADDR").ok().as_deref(),
            std::env::var("MINI_VPN_TUN_POOL_SIZE").ok().as_deref(),
        )
    }

    /// Derive the listener-pool blueprint from startup config.
    /// 中文要点：配置只负责输入，真正监听池蓝图仍然交给 `ListenerSpec`。
    fn listener_spec(&self) -> ListenerSpec {
        ListenerSpec {
            local_port: self.local_port,
            pool_size: self.pool_size,
        }
    }
}
```

- [ ] **Step 4: Run the default-config test to verify it passes**

Run:

```bash
cargo test tun_runtime_config_defaults_match_stage4_behavior -- --nocapture
```

Expected:

```text
test client_tun::tests::tun_runtime_config_defaults_match_stage4_behavior ... ok
```

- [ ] **Step 5: Commit the config-struct milestone**

Run:

```bash
git add src/client_tun.rs
git commit -m "feat(tun): add minimal runtime config struct"
```

Expected:

```text
[main ...] feat(tun): add minimal runtime config struct
```

## Task 2: Replace Direct Runtime Hardcoding

**Files:**
- Modify: `src/client_tun.rs`
- Test: `src/client_tun.rs`

- [ ] **Step 1: Write a derivation test for custom values**

Add this unit test:

```rust
#[test]
fn tun_runtime_config_derives_listener_spec_and_target() {
    let config = TunRuntimeConfig::from_sources(
        Some("8080"),
        Some("127.0.0.1:7897"),
        Some("2"),
    )
    .expect("config should load");

    let listener_spec = config.listener_spec();

    assert_eq!(listener_spec.local_port, 8080);
    assert_eq!(listener_spec.pool_size, 2);
    assert_eq!(config.target_addr.to_wire_string(), "127.0.0.1:7897");
}
```

- [ ] **Step 2: Run the derivation test to verify it passes**

Run:

```bash
cargo test tun_runtime_config_derives_listener_spec_and_target -- --nocapture
```

Expected:

```text
test client_tun::tests::tun_runtime_config_derives_listener_spec_and_target ... ok
```

- [ ] **Step 3: Update `start_tun_proxy()` to use `TunRuntimeConfig`**

Replace the current startup config block:

```rust
let listener_spec = ListenerSpec {
    local_port: DEFAULT_TUN_LISTEN_PORT,
    pool_size: DEFAULT_TUN_POOL_SIZE,
};
let default_target =
    TargetAddr::parse(DEFAULT_TUN_TARGET).expect("默认 TUN 目标地址必须合法");
```

with:

```rust
let runtime_config = match TunRuntimeConfig::from_env() {
    Ok(config) => config,
    Err(e) => {
        println!("加载 TUN 运行时配置失败: {e}");
        return;
    }
};
let listener_spec = runtime_config.listener_spec();
let default_target = runtime_config.target_addr.clone();
```

- [ ] **Step 4: Update startup logging to show effective config**

Change the startup log to include effective values:

```rust
println!(
    "🚀 TUN runtime started with local_port={}, pool_size={}, target={}",
    listener_spec.local_port,
    listener_spec.pool_size,
    default_target.to_wire_string()
);
```

Keep the Chinese-friendly context in surrounding comments or log copy if needed, but make the log itself concise and parseable.

- [ ] **Step 5: Run `cargo check` to verify the config-driven startup compiles**

Run:

```bash
cargo check
```

Expected:

```text
Finished `dev` profile ... target(s) in ...
```

- [ ] **Step 6: Commit the hardcoding-removal milestone**

Run:

```bash
git add src/client_tun.rs
git commit -m "refactor(tun): drive startup from runtime config"
```

Expected:

```text
[main ...] refactor(tun): drive startup from runtime config
```

## Task 3: Add Config Validation Tests

**Files:**
- Modify: `src/client_tun.rs`
- Test: `src/client_tun.rs`

- [ ] **Step 1: Add invalid-input unit tests**

Add these tests:

```rust
#[test]
fn tun_runtime_config_rejects_invalid_local_port() {
    let err = TunRuntimeConfig::from_sources(Some("abc"), None, None)
        .expect_err("invalid port should fail");
    assert!(err.to_string().contains("invalid local port"));
}

#[test]
fn tun_runtime_config_rejects_invalid_target_addr() {
    let err = TunRuntimeConfig::from_sources(None, Some("bad-target"), None)
        .expect_err("invalid target should fail");
    assert!(err.to_string().contains("invalid target"));
}

#[test]
fn tun_runtime_config_rejects_zero_pool_size() {
    let err = TunRuntimeConfig::from_sources(None, None, Some("0"))
        .expect_err("zero pool size should fail");
    assert!(err.to_string().contains("at least 1"));
}
```

- [ ] **Step 2: Run the targeted config tests**

Run:

```bash
cargo test tun_runtime_config_ -- --nocapture
```

Expected:

```text
All `tun_runtime_config_*` tests pass.
```

- [ ] **Step 3: Add one valid override test**

Add this test:

```rust
#[test]
fn tun_runtime_config_accepts_valid_override_values() {
    let config = TunRuntimeConfig::from_sources(
        Some("8081"),
        Some("www.figma.com:443"),
        Some("3"),
    )
    .expect("valid config should load");

    assert_eq!(config.local_port, 8081);
    assert_eq!(config.pool_size, 3);
    assert_eq!(config.target_addr.to_wire_string(), "www.figma.com:443");
}
```

- [ ] **Step 4: Run the config test subset again**

Run:

```bash
cargo test tun_runtime_config_ -- --nocapture
```

Expected:

```text
All `tun_runtime_config_*` tests pass, including the valid override case.
```

- [ ] **Step 5: Commit the config-validation milestone**

Run:

```bash
git add src/client_tun.rs
git commit -m "test(tun): add runtime config validation coverage"
```

Expected:

```text
[main ...] test(tun): add runtime config validation coverage
```

## Task 4: Write The Stage 5 Teaching Note

**Files:**
- Create: `docs/tech/05-tun-runtime-minimal-config.md`

- [ ] **Step 1: Write the teaching note**

Create `docs/tech/05-tun-runtime-minimal-config.md` with content shaped like:

```md
# 05 TUN Runtime Minimal Config

## 背景

Stage 4 把 TUN 监听池真正激活了，但运行时还有 3 个关键值是写死的：

- local port
- target address
- pool size

Stage 5 的目标不是做大而全配置系统，而是先把这 3 个硬编码拔掉。

## 这一步为什么只做最小配置版

如果一开始就把：

- `server_addr`
- `tls_sni`
- `client-direct`

也一起拉进来，改动面会明显扩大。

所以 Stage 5 先只做：

- `MINI_VPN_TUN_LOCAL_PORT`
- `MINI_VPN_TUN_TARGET_ADDR`
- `MINI_VPN_TUN_POOL_SIZE`

## 关键结构

`TunRuntimeConfig` 负责：

- 读取环境变量
- 应用默认值
- 校验非法输入
- 派生 `ListenerSpec`

## 示例

```bash
MINI_VPN_TUN_LOCAL_PORT=8080 \
MINI_VPN_TUN_TARGET_ADDR=127.0.0.1:7897 \
MINI_VPN_TUN_POOL_SIZE=2 \
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

- [ ] **Step 3: Commit the Stage 5 docs**

Run:

```bash
git add docs/tech/05-tun-runtime-minimal-config.md
git commit -m "docs(tun): add stage 5 runtime config note"
```

Expected:

```text
[main ...] docs(tun): add stage 5 runtime config note
```

## Task 5: Full Validation And Runtime Smoke Check

**Files:**
- Modify: `src/client_tun.rs` (only if last-minute fixes are required)
- Modify: `docs/tech/05-tun-runtime-minimal-config.md` (only if test commands need tuning)

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
./target/debug/mini_vpn client-tun
```

Expected:

```text
Startup log shows local_port=80, pool_size=4, target=httpbin.org:80
```

If the command is blocked by TUN permission requirements, record that limitation and continue with the configuration-specific dry-run checks below.

- [ ] **Step 3: Run an override startup smoke check**

Run:

```bash
MINI_VPN_TUN_LOCAL_PORT=8080 \
MINI_VPN_TUN_TARGET_ADDR=127.0.0.1:7897 \
MINI_VPN_TUN_POOL_SIZE=2 \
./target/debug/mini_vpn client-tun
```

Expected:

```text
Startup log shows local_port=8080, pool_size=2, target=127.0.0.1:7897
```

Again, if TUN permission blocks full startup, capture whether the config log is emitted before the permission boundary. If not, report that runtime smoke validation requires the user's privileged local environment.

- [ ] **Step 4: Verify git status and recent history**

Run:

```bash
git status --short
git log --oneline -n 8
```

Expected:

```text
Working tree clean, and recent commits include the Stage 5 config milestones.
```

- [ ] **Step 5: If needed, create one final umbrella commit**

Run only if there are remaining uncommitted Stage 5 changes:

```bash
git add src/client_tun.rs docs/tech/05-tun-runtime-minimal-config.md
git commit -m "feat(tun): add minimal runtime config"
```

Expected:

```text
[main ...] feat(tun): add minimal runtime config
```

## Self-Review

### Spec Coverage

- `TunRuntimeConfig`: covered by Tasks 1 and 2
- replacement of hardcoded runtime source: covered by Task 2
- invalid explicit config rejection: covered by Task 3
- English-led + Chinese-key-point comments: covered by Tasks 1 and 2
- Stage 4 acceptance close-out: already completed and referenced
- Stage 5 teaching note: covered by Task 4
- full validation: covered by Task 5

### Placeholder Scan

- No `TODO`, `TBD`, or vague “handle later” instructions remain
- Every step includes exact files, code, commands, and expected results

### Type Consistency

- `TunRuntimeConfig`, `from_sources()`, `from_env()`, and `listener_spec()` are used consistently across all tasks

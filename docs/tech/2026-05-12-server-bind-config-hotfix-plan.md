# Server Bind Config Hotfix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the relay server bind address configurable and remove the client TUN upstream connect panic so local end-to-end tests can be run with explicit ports.

**Architecture:** Add a tiny server-side runtime config that owns only the bind address and validates it at startup. Keep the existing TLS/Yamux relay flow unchanged, and downgrade the client-side upstream TCP connect failure from panic to explicit log-and-return.

**Tech Stack:** Rust, Tokio, rustls, tokio-rustls, yamux

---

### Task 1: Server Bind Config

**Files:**
- Modify: `/Users/liushan/Documents/Personal/Languages/Rust/mini_vpn/src/server.rs`

- [ ] **Step 1: Add a failing config test**

```rust
#[test]
fn server_runtime_config_accepts_valid_bind_addr() {
    let config = ServerRuntimeConfig::from_sources(Some("127.0.0.1:9000"))
        .expect("config should load");
    assert_eq!(config.bind_addr, "127.0.0.1:9000");
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test server_runtime_config_accepts_valid_bind_addr -- --nocapture`
Expected: FAIL because `ServerRuntimeConfig` does not exist yet

- [ ] **Step 3: Write minimal implementation**

```rust
const DEFAULT_SERVER_BIND_ADDR: &str = "127.0.0.1:8081";

#[derive(Debug, Clone)]
struct ServerRuntimeConfig {
    bind_addr: String,
}

impl ServerRuntimeConfig {
    fn from_sources(bind_addr: Option<&str>) -> Result<Self, String> {
        let bind_addr = bind_addr.unwrap_or(DEFAULT_SERVER_BIND_ADDR).to_string();
        bind_addr
            .parse::<std::net::SocketAddr>()
            .map_err(|_| format!("invalid server bind addr: {bind_addr}"))?;
        Ok(Self { bind_addr })
    }
}
```

- [ ] **Step 4: Run focused tests**

Run: `cargo test server_runtime_config -- --nocapture`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add /Users/liushan/Documents/Personal/Languages/Rust/mini_vpn/src/server.rs
git commit -m "feat(server): make bind address configurable"
```

### Task 2: Client Upstream Connect Error

**Files:**
- Modify: `/Users/liushan/Documents/Personal/Languages/Rust/mini_vpn/src/client_tun.rs`

- [ ] **Step 1: Replace panic-based connect**

```rust
let server_stream = match TcpStream::connect(upstream_server_addr.as_str()).await {
    Ok(stream) => stream,
    Err(e) => {
        println!("连接代理服务端失败 {upstream_server_addr}: {e}");
        return;
    }
};
```

- [ ] **Step 2: Run validation**

Run: `cargo check && cargo clippy --all-targets --all-features -- -D warnings`
Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add /Users/liushan/Documents/Personal/Languages/Rust/mini_vpn/src/client_tun.rs
git commit -m "refactor(tun): avoid panic on upstream connect failure"
```

### Task 3: Docs And Local Scripts

**Files:**
- Create: `/Users/liushan/Documents/Personal/Languages/Rust/mini_vpn/docs/tech/07-server-bind-and-client-connect-errors.md`

- [ ] **Step 1: Write the teaching note**

```md
Explain:
- why `client-tun` override succeeded
- why `server.rs` still listened on 8081 before the hotfix
- how to launch server and client with matching ports
```

- [ ] **Step 2: Run docs-friendly validation**

Run: `cargo test && cargo doc --no-deps`
Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add /Users/liushan/Documents/Personal/Languages/Rust/mini_vpn/docs/tech/07-server-bind-and-client-connect-errors.md
git commit -m "docs(server): add bind config and test script note"
```

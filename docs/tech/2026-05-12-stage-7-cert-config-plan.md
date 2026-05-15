# Stage 7 Cert Config Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add configurable TLS material paths for `server` and `client-tun`, plus a local SAN test certificate script, so address, SNI, and certificate inputs can be aligned without editing source code.

**Architecture:** Keep config layering small and explicit. `server.rs` will own `ServerRuntimeConfig` for bind address and `ServerTlsConfig` for certificate files; `client_tun.rs` will keep listener/upstream config and add `TunTlsConfig` for CA path. Startup performs path and PEM validation before entering the TLS/Yamux hot path, and a small script generates local SAN certificates for `localhost` and `example.com`.

**Tech Stack:** Rust, Tokio, rustls, tokio-rustls, rustls-pemfile, shell script, OpenSSL

---

## File Map

- Modify: `/Users/liushan/Documents/Personal/Languages/Rust/mini_vpn/src/server.rs`
  - Add `ServerTlsConfig`
  - Load `cert_path` and `key_path` from env/defaults
  - Replace direct `File::open("cert.pem")` / `File::open("key.pem")`
  - Add focused unit tests for TLS config parsing
- Modify: `/Users/liushan/Documents/Personal/Languages/Rust/mini_vpn/src/client_tun.rs`
  - Add `TunTlsConfig`
  - Load `ca_path` from env/defaults
  - Replace direct `File::open("cert.pem")`
  - Add focused unit tests for CA path parsing
- Create: `/Users/liushan/Documents/Personal/Languages/Rust/mini_vpn/scripts/gen-test-certs.sh`
  - Generate local SAN dev certs for `localhost`, `example.com`, and `127.0.0.1`
- Create: `/Users/liushan/Documents/Personal/Languages/Rust/mini_vpn/docs/tech/07-cert-path-and-sni-alignment.md`
  - Explain config fields, script usage, and local test commands

### Task 1: Server TLS Config

**Files:**
- Modify: `/Users/liushan/Documents/Personal/Languages/Rust/mini_vpn/src/server.rs`

- [ ] **Step 1: Write the failing tests**

Add these tests under the existing `#[cfg(test)]` module:

```rust
    #[test]
    fn server_tls_config_defaults_match_existing_behavior() {
        let config = ServerTlsConfig::from_sources(None, None).expect("config should load");
        assert_eq!(config.cert_path, "cert.pem");
        assert_eq!(config.key_path, "key.pem");
    }

    #[test]
    fn server_tls_config_accepts_override_paths() {
        let config = ServerTlsConfig::from_sources(
            Some("certs/dev/server-cert.pem"),
            Some("certs/dev/server-key.pem"),
        )
        .expect("config should load");
        assert_eq!(config.cert_path, "certs/dev/server-cert.pem");
        assert_eq!(config.key_path, "certs/dev/server-key.pem");
    }
```

- [ ] **Step 2: Run focused test to verify it fails**

Run:

```bash
cargo test server_tls_config -- --nocapture
```

Expected: FAIL because `ServerTlsConfig` does not exist yet.

- [ ] **Step 3: Write minimal implementation**

Add the config type and env loader near `ServerRuntimeConfig`:

```rust
const DEFAULT_SERVER_CERT_PATH: &str = "cert.pem";
const DEFAULT_SERVER_KEY_PATH: &str = "key.pem";

#[derive(Debug, Clone)]
struct ServerTlsConfig {
    cert_path: String,
    key_path: String,
}

impl ServerTlsConfig {
    fn from_sources(cert_path: Option<&str>, key_path: Option<&str>) -> Result<Self, String> {
        let cert_path = cert_path.unwrap_or(DEFAULT_SERVER_CERT_PATH).to_string();
        let key_path = key_path.unwrap_or(DEFAULT_SERVER_KEY_PATH).to_string();
        if cert_path.trim().is_empty() {
            return Err("invalid server cert path: empty".to_string());
        }
        if key_path.trim().is_empty() {
            return Err("invalid server key path: empty".to_string());
        }
        Ok(Self { cert_path, key_path })
    }

    fn from_env() -> Result<Self, String> {
        let cert_path = std::env::var("MINI_VPN_SERVER_CERT_PATH").ok();
        let key_path = std::env::var("MINI_VPN_SERVER_KEY_PATH").ok();
        Self::from_sources(cert_path.as_deref(), key_path.as_deref())
    }
}
```

Then wire it into `run()`:

```rust
    let tls_config = match ServerTlsConfig::from_env() {
        Ok(config) => config,
        Err(e) => {
            println!("加载服务端 TLS 配置失败: {e}");
            return;
        }
    };

    println!(
        "运行服务器端，监听地址: {}, cert_path: {}, key_path: {}",
        runtime_config.bind_addr, tls_config.cert_path, tls_config.key_path
    );
```

And replace the hardcoded file opens:

```rust
    let cert_file = match File::open(tls_config.cert_path.as_str()) {
        Ok(file) => file,
        Err(e) => {
            println!("打开服务端证书失败 {}: {e}", tls_config.cert_path);
            return;
        }
    };
    let key_file = match File::open(tls_config.key_path.as_str()) {
        Ok(file) => file,
        Err(e) => {
            println!("打开服务端私钥失败 {}: {e}", tls_config.key_path);
            return;
        }
    };
    let cert_file = &mut BufReader::new(cert_file);
    let key_file = &mut BufReader::new(key_file);
```

- [ ] **Step 4: Run focused tests to verify it passes**

Run:

```bash
cargo test server_tls_config -- --nocapture
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add /Users/liushan/Documents/Personal/Languages/Rust/mini_vpn/src/server.rs
git commit -m "feat(server): add configurable tls material paths"
```

### Task 2: Client TUN CA Config

**Files:**
- Modify: `/Users/liushan/Documents/Personal/Languages/Rust/mini_vpn/src/client_tun.rs`

- [ ] **Step 1: Write the failing tests**

Add these tests in the existing client test module:

```rust
    #[test]
    fn tun_tls_config_defaults_match_existing_behavior() {
        let config = TunTlsConfig::from_sources(None).expect("config should load");
        assert_eq!(config.ca_path, "cert.pem");
    }

    #[test]
    fn tun_tls_config_accepts_override_path() {
        let config = TunTlsConfig::from_sources(Some("certs/dev/ca-cert.pem"))
            .expect("config should load");
        assert_eq!(config.ca_path, "certs/dev/ca-cert.pem");
    }
```

- [ ] **Step 2: Run focused test to verify it fails**

Run:

```bash
cargo test tun_tls_config -- --nocapture
```

Expected: FAIL because `TunTlsConfig` does not exist yet.

- [ ] **Step 3: Write minimal implementation**

Add the config type near `TunUpstreamConfig`:

```rust
const DEFAULT_TUN_CA_PATH: &str = "cert.pem";

#[derive(Debug, Clone)]
struct TunTlsConfig {
    ca_path: String,
}

impl TunTlsConfig {
    fn from_sources(ca_path: Option<&str>) -> Result<Self, ClientError> {
        let ca_path = ca_path.unwrap_or(DEFAULT_TUN_CA_PATH).to_string();
        if ca_path.trim().is_empty() {
            return Err(ClientError::InvalidTarget(
                "invalid tun ca path: empty".to_string(),
            ));
        }
        Ok(Self { ca_path })
    }
}
```

Extend `TunRuntimeConfig`:

```rust
struct TunRuntimeConfig {
    listener: TunListenerConfig,
    upstream: TunUpstreamConfig,
    tls: TunTlsConfig,
}
```

Update builders:

```rust
    fn from_sources(
        local_port: Option<&str>,
        target_addr: Option<&str>,
        pool_size: Option<&str>,
        server_addr: Option<&str>,
        tls_sni: Option<&str>,
        ca_path: Option<&str>,
    ) -> Result<Self, ClientError> {
        Ok(Self {
            listener: TunListenerConfig::from_sources(local_port, target_addr, pool_size)?,
            upstream: TunUpstreamConfig::from_sources(server_addr, tls_sni)?,
            tls: TunTlsConfig::from_sources(ca_path)?,
        })
    }
```

Read the env var and load CA file through config:

```rust
        let ca_path = std::env::var("MINI_VPN_TUN_CA_PATH").ok();
```

```rust
    let tls_ca_path = runtime_config.tls.ca_path.clone();
    let cert_file = match File::open(tls_ca_path.as_str()) {
        Ok(file) => file,
        Err(e) => {
            println!("打开客户端 CA 证书失败 {}: {e}", tls_ca_path);
            return;
        }
    };
    let cert_file = &mut BufReader::new(cert_file);
```

And include `ca_path` in startup log:

```rust
        "🚀 TUN runtime started with local_port={}, pool_size={}, target={}, server_addr={}, tls_sni={}, ca_path={}",
```

- [ ] **Step 4: Run focused tests to verify it passes**

Run:

```bash
cargo test tun_tls_config -- --nocapture
```

Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add /Users/liushan/Documents/Personal/Languages/Rust/mini_vpn/src/client_tun.rs
git commit -m "feat(tun): add configurable ca path"
```

### Task 3: Dev SAN Certificate Script

**Files:**
- Create: `/Users/liushan/Documents/Personal/Languages/Rust/mini_vpn/scripts/gen-test-certs.sh`

- [ ] **Step 1: Write the script**

Create this script:

```bash
#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT_DIR="${ROOT_DIR}/certs/dev"
mkdir -p "${OUT_DIR}"

openssl req -x509 -newkey rsa:2048 -sha256 -days 365 -nodes \
  -keyout "${OUT_DIR}/server-key.pem" \
  -out "${OUT_DIR}/server-cert.pem" \
  -subj "/CN=localhost" \
  -addext "subjectAltName=DNS:localhost,DNS:example.com,IP:127.0.0.1"

cp "${OUT_DIR}/server-cert.pem" "${OUT_DIR}/ca-cert.pem"

echo "Generated:"
echo "  ${OUT_DIR}/server-cert.pem"
echo "  ${OUT_DIR}/server-key.pem"
echo "  ${OUT_DIR}/ca-cert.pem"
```

- [ ] **Step 2: Make the script executable**

Run:

```bash
chmod +x /Users/liushan/Documents/Personal/Languages/Rust/mini_vpn/scripts/gen-test-certs.sh
```

Expected: command succeeds with no output.

- [ ] **Step 3: Run the script**

Run:

```bash
/Users/liushan/Documents/Personal/Languages/Rust/mini_vpn/scripts/gen-test-certs.sh
```

Expected: `certs/dev/server-cert.pem`, `server-key.pem`, and `ca-cert.pem` are created.

- [ ] **Step 4: Commit**

```bash
git add /Users/liushan/Documents/Personal/Languages/Rust/mini_vpn/scripts/gen-test-certs.sh
git commit -m "build(tls): add dev san certificate script"
```

### Task 4: Docs And End-To-End Validation

**Files:**
- Create: `/Users/liushan/Documents/Personal/Languages/Rust/mini_vpn/docs/tech/07-cert-path-and-sni-alignment.md`
- Modify: `/Users/liushan/Documents/Personal/Languages/Rust/mini_vpn/src/server.rs`
- Modify: `/Users/liushan/Documents/Personal/Languages/Rust/mini_vpn/src/client_tun.rs`

- [ ] **Step 1: Write the teaching note**

Document:

```md
- which env vars configure server TLS materials
- which env vars configure client TUN CA path
- how to generate SAN test certs
- how to run localhost and example.com local tests
```

- [ ] **Step 2: Run the full validation suite**

Run:

```bash
cargo test
cargo check
cargo clippy --all-targets --all-features -- -D warnings
cargo doc --no-deps
```

Expected: all commands PASS.

- [ ] **Step 3: Run default local validation**

Run server:

```bash
cargo run -- server
```

Run client in another terminal:

```bash
sudo ./target/debug/mini_vpn client-tun
```

Expected: default `127.0.0.1:8081 + localhost + cert.pem/key.pem` path works.

- [ ] **Step 4: Run `example.com` SAN local validation**

Run script first:

```bash
./scripts/gen-test-certs.sh
```

Run server:

```bash
export MINI_VPN_SERVER_BIND_ADDR=127.0.0.1:9000
export MINI_VPN_SERVER_CERT_PATH=certs/dev/server-cert.pem
export MINI_VPN_SERVER_KEY_PATH=certs/dev/server-key.pem
cargo run -- server
```

Run client:

```bash
export MINI_VPN_TUN_SERVER_ADDR=127.0.0.1:9000
export MINI_VPN_TUN_TLS_SNI=example.com
export MINI_VPN_TUN_CA_PATH=certs/dev/ca-cert.pem
sudo -E ./target/debug/mini_vpn client-tun
```

Expected: TLS handshake succeeds with `example.com`.

- [ ] **Step 5: Commit**

```bash
git add /Users/liushan/Documents/Personal/Languages/Rust/mini_vpn/docs/tech/07-cert-path-and-sni-alignment.md
git commit -m "docs(tls): add cert path and sni alignment note"
```

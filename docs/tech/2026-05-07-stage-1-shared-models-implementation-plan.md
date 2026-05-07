# Stage 1 Shared Models And Relay Protocol Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the shared target model, relay handshake protocol, typed errors, and reusable Yamux client-session opener so both `client.rs` and `client_tun.rs` can stop duplicating target and handshake logic.

**Architecture:** Introduce a focused `shared` module tree under `src/shared/` and move all target-address parsing, relay-request serialization, fake-header handshake, and Yamux substream opening into that layer. Keep the stage narrow: Stage 1 does not refactor the full direct proxy or TUN runtime yet, but it must compile cleanly and include tests that prove client/server handshake interoperability.

**Tech Stack:** Rust 2024, Tokio, tokio-rustls, yamux, tokio-util compat, bytes, thiserror

---

## File Map

- Create: `src/shared/mod.rs`
- Create: `src/shared/target.rs`
- Create: `src/shared/relay_protocol.rs`
- Create: `src/shared/tunnel.rs`
- Create: `src/shared/errors.rs`
- Create: `src/lib.rs`
- Create: `tests/shared_relay_protocol.rs`
- Modify: `Cargo.toml`
- Modify: `src/main.rs`
- Create after implementation: `docs/tech/01-shared-models-and-relay-protocol.md`

## Design Notes Locked For This Stage

- Keep the camouflage header exactly as it exists today:

```rust
pub const FAKE_HTTP_HEADER: &[u8; 38] = b"GET / HTTP/1.1\r\nHost: www.bing.com\r\n\r\n";
```

- Replace stringly-typed target passing with `TargetAddr`.
- Replace branch-local handshake code with `RelayRequest`.
- Preserve the newline-delimited wire format for now to minimize server churn in Stage 2.
- Add `thiserror` now; delay `anyhow` introduction until later if still needed.

### Task 1: Add Shared Module Scaffolding And Error Dependency

**Files:**
- Modify: `Cargo.toml`
- Modify: `src/main.rs`
- Create: `src/shared/mod.rs`
- Create: `src/lib.rs`

- [ ] **Step 1: Add the `thiserror` dependency**

Update `Cargo.toml` dependencies block by adding:

```toml
thiserror = "1.0"
```

- [ ] **Step 2: Register the shared module in `main.rs`**

Update `src/main.rs` to:

```rust
mod client;
mod client_tun;
mod device;
mod server;
mod shared;

#[tokio::main]
async fn main() {
    let mode = std::env::args()
        .nth(1)
        .expect("请指定运行模式: client 或 server");

    if mode == "server" {
        server::run().await;
    } else if mode == "client" {
        client_tun::start_tun_proxy().await;
    }
}
```

- [ ] **Step 3: Create the shared module root**

Create `src/shared/mod.rs`:

```rust
pub mod errors;
pub mod relay_protocol;
pub mod target;
pub mod tunnel;

pub use errors::ClientError;
pub use relay_protocol::{
    read_relay_request,
    write_relay_request,
    RelayRequest,
    FAKE_HTTP_HEADER,
};
pub use target::TargetAddr;
pub use tunnel::open_remote_session;
```

- [ ] **Step 4: Create the library entry for integration tests**

Create `src/lib.rs`:

```rust
pub mod shared;
```

- [ ] **Step 5: Run `cargo check` to verify scaffolding fails for missing files**

Run:

```bash
cargo check
```

Expected:

- FAIL with module-not-found errors for `errors`, `relay_protocol`, `target`, and `tunnel`

- [ ] **Step 6: Commit scaffolding**

Run:

```bash
git add Cargo.toml src/main.rs src/lib.rs src/shared/mod.rs
git commit -m "refactor: add shared module scaffolding"
```

### Task 2: Implement Typed Shared Errors

**Files:**
- Create: `src/shared/errors.rs`
- Modify: `src/shared/mod.rs`
- Test: `cargo check`

- [ ] **Step 1: Create the shared error type**

Create `src/shared/errors.rs`:

```rust
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ClientError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("invalid target address: {0}")]
    InvalidTarget(String),

    #[error("unsupported SOCKS address type: {0}")]
    UnsupportedAddressType(u8),

    #[error("invalid relay request: {0}")]
    InvalidRelayRequest(String),

    #[error("yamux open stream failed: {0}")]
    YamuxOpen(#[source] yamux::ConnectionError),

    #[error("invalid utf-8 payload: {0}")]
    Utf8(#[from] std::string::FromUtf8Error),
}
```

- [ ] **Step 2: Verify re-export remains correct**

Keep `src/shared/mod.rs` export section as:

```rust
pub use errors::ClientError;
pub use relay_protocol::{
    read_relay_request,
    write_relay_request,
    RelayRequest,
    FAKE_HTTP_HEADER,
};
pub use target::TargetAddr;
pub use tunnel::open_remote_session;
```

- [ ] **Step 3: Run `cargo check`**

Run:

```bash
cargo check
```

Expected:

- FAIL only for still-missing modules and unresolved shared exports
- no syntax error in `errors.rs`

- [ ] **Step 4: Commit typed errors**

Run:

```bash
git add src/shared/errors.rs src/shared/mod.rs
git commit -m "refactor: add shared client error type"
```

### Task 3: Implement The Structured Target Model

**Files:**
- Create: `src/shared/target.rs`
- Create: `tests/shared_relay_protocol.rs`

- [ ] **Step 1: Write the failing target tests**

Create `tests/shared_relay_protocol.rs` with:

```rust
use mini_vpn::shared::TargetAddr;

#[test]
fn parses_ipv4_target() {
    let target = TargetAddr::parse("127.0.0.1:7897").expect("target should parse");
    assert_eq!(target.to_wire_string(), "127.0.0.1:7897");
}

#[test]
fn parses_domain_target() {
    let target = TargetAddr::parse("www.figma.com:443").expect("target should parse");
    assert_eq!(target.to_wire_string(), "www.figma.com:443");
}

#[test]
fn rejects_missing_port() {
    let err = TargetAddr::parse("www.figma.com").expect_err("port is required");
    assert!(err.to_string().contains("invalid target address"));
}
```

- [ ] **Step 2: Run the failing tests**

Run:

```bash
cargo test parses_ipv4_target --test shared_relay_protocol
```

Expected:

- FAIL because `TargetAddr` is not implemented yet

- [ ] **Step 3: Implement `TargetAddr`**

Create `src/shared/target.rs`:

```rust
use crate::shared::errors::ClientError;

/// Structured target address for direct proxy and TUN relay requests.
/// 中文要点：统一承载 IP:port 与域名:port，避免热路径里拼接裸字符串。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TargetAddr {
    IpPort(std::net::SocketAddr),
    DomainPort { host: String, port: u16 },
}

impl TargetAddr {
    /// Parse a target string into a structured target model.
    /// 中文要点：优先解析为 `SocketAddr`，失败后退化为域名加端口解析。
    pub fn parse(input: &str) -> Result<Self, ClientError> {
        if let Ok(addr) = input.parse::<std::net::SocketAddr>() {
            return Ok(Self::IpPort(addr));
        }

        let (host, port_str) = input
            .rsplit_once(':')
            .ok_or_else(|| ClientError::InvalidTarget(input.to_string()))?;
        let port = port_str
            .parse::<u16>()
            .map_err(|_| ClientError::InvalidTarget(input.to_string()))?;

        if host.is_empty() {
            return Err(ClientError::InvalidTarget(input.to_string()));
        }

        Ok(Self::DomainPort {
            host: host.to_string(),
            port,
        })
    }

    /// Render the target in the current wire format.
    /// 中文要点：阶段一沿用现有换行分隔协议，输出 `host:port` 文本。
    pub fn to_wire_string(&self) -> String {
        match self {
            Self::IpPort(addr) => addr.to_string(),
            Self::DomainPort { host, port } => format!("{host}:{port}"),
        }
    }
}
```

- [ ] **Step 4: Run all target tests**

Run:

```bash
cargo test --test shared_relay_protocol
```

Expected:

- PASS for `parses_ipv4_target`
- PASS for `parses_domain_target`
- PASS for `rejects_missing_port`

- [ ] **Step 5: Commit target model**

Run:

```bash
git add src/shared/target.rs tests/shared_relay_protocol.rs
git commit -m "refactor: add shared target address model"
```

### Task 4: Implement Relay Request Serialization And Parsing

**Files:**
- Create: `src/shared/relay_protocol.rs`
- Modify: `tests/shared_relay_protocol.rs`

- [ ] **Step 1: Extend the failing integration tests for relay requests**

Append to `tests/shared_relay_protocol.rs`:

```rust
use mini_vpn::shared::{read_relay_request, write_relay_request, RelayRequest};
use tokio::io::duplex;

#[tokio::test]
async fn tcp_request_round_trip() {
    let request = RelayRequest::Tcp {
        target: TargetAddr::parse("34.107.238.235:443").expect("target should parse"),
    };
    let (client, server) = duplex(256);

    let writer = tokio::spawn(async move {
        let mut client = client;
        write_relay_request(&mut client, &request)
            .await
            .expect("write should succeed");
    });

    let reader = tokio::spawn(async move {
        let mut server = server;
        read_relay_request(&mut server)
            .await
            .expect("read should succeed")
    });

    writer.await.expect("writer task should join");
    let received = reader.await.expect("reader task should join");
    assert_eq!(received, request);
}

#[tokio::test]
async fn udp_request_round_trip() {
    let request = RelayRequest::Udp { target: None };
    let (client, server) = duplex(256);

    let writer = tokio::spawn(async move {
        let mut client = client;
        write_relay_request(&mut client, &request)
            .await
            .expect("write should succeed");
    });

    let reader = tokio::spawn(async move {
        let mut server = server;
        read_relay_request(&mut server)
            .await
            .expect("read should succeed")
    });

    writer.await.expect("writer task should join");
    let received = reader.await.expect("reader task should join");
    assert_eq!(received, request);
}
```

- [ ] **Step 2: Run the failing relay tests**

Run:

```bash
cargo test tcp_request_round_trip --test shared_relay_protocol
```

Expected:

- FAIL because `RelayRequest`, `read_relay_request`, and `write_relay_request` do not exist yet

- [ ] **Step 3: Implement the relay protocol**

Create `src/shared/relay_protocol.rs`:

```rust
use crate::shared::{ClientError, TargetAddr};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};

pub const FAKE_HTTP_HEADER: &[u8; 38] = b"GET / HTTP/1.1\r\nHost: www.bing.com\r\n\r\n";

/// Relay request exchanged over a Yamux substream after the fake HTTP header.
/// 中文要点：阶段一沿用文本协议，后续可升级为二进制帧，但外层调用接口保持不变。
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelayRequest {
    Tcp { target: TargetAddr },
    Udp { target: Option<TargetAddr> },
}

/// Write the shared relay request payload.
/// 中文要点：先写伪装头，再写一行文本请求。
pub async fn write_relay_request<W>(
    writer: &mut W,
    request: &RelayRequest,
) -> Result<(), ClientError>
where
    W: AsyncWrite + Unpin,
{
    writer.write_all(FAKE_HTTP_HEADER).await?;

    let line = match request {
        RelayRequest::Tcp { target } => format!("TCP {}\n", target.to_wire_string()),
        RelayRequest::Udp { target: Some(target) } => format!("UDP {}\n", target.to_wire_string()),
        RelayRequest::Udp { target: None } => "UDP\n".to_string(),
    };

    writer.write_all(line.as_bytes()).await?;
    writer.flush().await?;
    Ok(())
}

/// Read and validate a relay request.
/// 中文要点：校验伪装头后，再解析一行请求文本。
pub async fn read_relay_request<R>(reader: &mut R) -> Result<RelayRequest, ClientError>
where
    R: AsyncRead + Unpin,
{
    let mut magic_buf = [0u8; 38];
    tokio::io::AsyncReadExt::read_exact(reader, &mut magic_buf).await?;

    if &magic_buf != FAKE_HTTP_HEADER {
        return Err(ClientError::InvalidRelayRequest(
            "fake header mismatch".to_string(),
        ));
    }

    let mut buffered = BufReader::new(reader);
    let mut line = String::new();
    let bytes_read = buffered.read_line(&mut line).await?;

    if bytes_read == 0 {
        return Err(ClientError::InvalidRelayRequest(
            "empty relay request".to_string(),
        ));
    }

    let line = line.trim_end_matches('\n').trim_end_matches('\r');

    if line == "UDP" {
        return Ok(RelayRequest::Udp { target: None });
    }

    if let Some(target) = line.strip_prefix("TCP ") {
        return Ok(RelayRequest::Tcp {
            target: TargetAddr::parse(target)?,
        });
    }

    if let Some(target) = line.strip_prefix("UDP ") {
        return Ok(RelayRequest::Udp {
            target: Some(TargetAddr::parse(target)?),
        });
    }

    Err(ClientError::InvalidRelayRequest(line.to_string()))
}
```

- [ ] **Step 4: Run the relay request tests**

Run:

```bash
cargo test --test shared_relay_protocol tcp_request_round_trip
cargo test --test shared_relay_protocol udp_request_round_trip
```

Expected:

- PASS for both tests

- [ ] **Step 5: Commit relay protocol**

Run:

```bash
git add src/shared/relay_protocol.rs tests/shared_relay_protocol.rs
git commit -m "refactor: add shared relay request protocol"
```

### Task 5: Implement The Yamux Remote Session Helper

**Files:**
- Create: `src/shared/tunnel.rs`
- Modify: `tests/shared_relay_protocol.rs`

- [ ] **Step 1: Add a test for writing a full remote session handshake**

Append to `tests/shared_relay_protocol.rs`:

```rust
use mini_vpn::shared::FAKE_HTTP_HEADER;

#[tokio::test]
async fn write_relay_request_starts_with_fake_header() {
    let request = RelayRequest::Tcp {
        target: TargetAddr::parse("127.0.0.1:7897").expect("target should parse"),
    };
    let (client, mut server) = duplex(256);

    let writer = tokio::spawn(async move {
        let mut client = client;
        write_relay_request(&mut client, &request)
            .await
            .expect("write should succeed");
    });

    let mut magic = [0u8; 38];
    tokio::io::AsyncReadExt::read_exact(&mut server, &mut magic)
        .await
        .expect("server should read fake header");

    writer.await.expect("writer task should join");
    assert_eq!(&magic, FAKE_HTTP_HEADER);
}
```

- [ ] **Step 2: Run the new header test**

Run:

```bash
cargo test --test shared_relay_protocol write_relay_request_starts_with_fake_header
```

Expected:

- PASS if Task 4 is already correct

- [ ] **Step 3: Implement the remote session helper**

Create `src/shared/tunnel.rs`:

```rust
use crate::shared::{write_relay_request, ClientError, RelayRequest};
use tokio_util::compat::{FuturesAsyncReadCompatExt, TokioAsyncReadCompatExt};

/// Open a Yamux substream and send the shared relay handshake.
/// 中文要点：统一负责开子流、接转换器、写入共享握手，外层不再重复发暗号和目标。
pub async fn open_remote_session(
    ctrl: &mut yamux::Control,
    request: &RelayRequest,
) -> Result<tokio_util::compat::Compat<yamux::Stream>, ClientError> {
    let stream = ctrl.open_stream().await.map_err(ClientError::YamuxOpen)?;
    let mut stream = stream.compat();
    write_relay_request(&mut stream, request).await?;
    Ok(stream)
}
```

- [ ] **Step 4: Run the shared test suite**

Run:

```bash
cargo test --test shared_relay_protocol
```

Expected:

- PASS for all shared protocol tests

- [ ] **Step 5: Run crate checks**

Run:

```bash
cargo check
```

Expected:

- PASS

- [ ] **Step 6: Commit the tunnel helper**

Run:

```bash
git add src/shared/tunnel.rs
git commit -m "refactor: add shared yamux session opener"
```

### Task 6: Write Stage 1 Teaching Document

**Files:**
- Create: `docs/tech/01-shared-models-and-relay-protocol.md`

- [ ] **Step 1: Create the teaching document**

Create `docs/tech/01-shared-models-and-relay-protocol.md`:

```md
# 01 Shared Models And Relay Protocol

## 背景

在本阶段之前，`client.rs` 与 `client_tun.rs` 各自维护目标地址拼接、fake header 发送、目标协议协商，导致：

- 逻辑重复
- 分支漂移
- 后续连接池改造无法共享协议层

## 本阶段做了什么

- 新增 `TargetAddr`
- 新增 `RelayRequest`
- 新增统一 fake header 与握手读写函数
- 新增 Yamux 子流统一打开函数
- 新增共享错误类型

## 为什么这么设计

- 先统一协议层，再重构 DirectProxy 与 TunGateway
- 把“目标地址”和“握手协议”从业务分支中抽离
- 让后续所有模式都复用同一套远端会话打开逻辑

## 数据流

```text
caller
-> build TargetAddr
-> build RelayRequest
-> open_remote_session()
-> Yamux substream
-> fake header + request line
-> server parses shared protocol
```

## 关键权衡

- 本阶段继续保留文本协议，减少 server 改动面
- 没有一步到位改二进制协议，因为当前首要目标是去重与统一
- `thiserror` 先落地，先消除热路径 panic 风险的基础设施缺口

## 下一阶段

- `client.rs` 切换到共享握手
- 删掉原地拼接目标地址和手写 fake header 的代码
```

- [ ] **Step 2: Verify the doc exists and is readable**

Run:

```bash
test -f docs/tech/01-shared-models-and-relay-protocol.md && echo OK
```

Expected:

- output contains `OK`

- [ ] **Step 3: Commit the teaching document**

Run:

```bash
git add docs/tech/01-shared-models-and-relay-protocol.md
git commit -m "docs: add stage 1 teaching note"
```

### Task 7: Final Validation

**Files:**
- Test: `tests/shared_relay_protocol.rs`
- Test: whole crate

- [ ] **Step 1: Run the focused shared tests**

Run:

```bash
cargo test --test shared_relay_protocol
```

Expected:

- all shared protocol tests PASS

- [ ] **Step 2: Run full crate checks**

Run:

```bash
cargo check
```

Expected:

- PASS

- [ ] **Step 3: Run Clippy**

Run:

```bash
cargo clippy --all-targets --all-features -- -D warnings
```

Expected:

- PASS with zero warnings

- [ ] **Step 4: Run docs build**

Run:

```bash
cargo doc --no-deps
```

Expected:

- PASS

- [ ] **Step 5: Final commit for Stage 1**

Run:

```bash
git add Cargo.toml src/main.rs src/lib.rs src/shared tests/shared_relay_protocol.rs docs/tech/01-shared-models-and-relay-protocol.md
git commit -m "feat: add shared relay models and protocol"
```

## Self-Review

### Spec Coverage

- Shared target model: covered in Task 3
- Shared relay request and fake header handshake: covered in Task 4
- Shared Yamux session opener: covered in Task 5
- Typed shared errors: covered in Task 2
- Teaching doc requirement: covered in Task 6

### Placeholder Scan

- No `TODO`
- No `TBD`
- No unnamed “appropriate handling” steps

### Type Consistency

- `TargetAddr` is defined in Task 3 and reused consistently in Tasks 4 and 5
- `RelayRequest` is defined in Task 4 and reused consistently in Tasks 5 and 6
- `ClientError` is introduced in Task 2 and reused consistently afterward
- `src/lib.rs` only exposes `shared`, which is enough for Stage 1 integration tests and avoids widening the public surface unnecessarily

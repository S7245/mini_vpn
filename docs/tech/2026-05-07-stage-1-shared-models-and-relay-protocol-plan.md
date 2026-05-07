# Stage 1 Shared Models And Relay Protocol Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build shared target/protocol/error modules for `mini_vpn` so direct proxy mode and TUN mode can stop hardcoding target strings and stop duplicating tunnel handshake framing.

**Architecture:** Introduce a new `src/shared/` module tree with focused files for target modeling, relay request framing, and shared tunnel constants. Keep Stage 1 narrowly scoped: add reusable building blocks plus unit tests, without yet rewriting the large adapter files beyond wiring the new module tree into the crate.

**Tech Stack:** Rust 2024, Tokio, bytes, tokio-util, yamux, tokio-rustls, thiserror, cargo test

---

## File Structure

- Create: `src/shared/mod.rs`
- Create: `src/shared/target.rs`
- Create: `src/shared/relay_protocol.rs`
- Create: `src/shared/errors.rs`
- Modify: `src/main.rs`
- Modify: `Cargo.toml`
- Test: `src/shared/target.rs`
- Test: `src/shared/relay_protocol.rs`
- Document: `docs/tech/01-shared-models-and-relay-protocol.md`

### Responsibility Map

- `src/shared/mod.rs`
  - re-export shared modules
- `src/shared/target.rs`
  - define `TargetAddr`
  - serialize/parse stable target strings
  - own target unit tests
- `src/shared/relay_protocol.rs`
  - define `TransportKind`, `RelayRequest`
  - define fake header constant
  - encode/decode request payload after fake header
  - own protocol unit tests
- `src/shared/errors.rs`
  - define `ClientError`
- `src/main.rs`
  - register `shared` module
- `Cargo.toml`
  - add `thiserror`
- `docs/tech/01-shared-models-and-relay-protocol.md`
  - teaching summary for Stage 1

### Notes Before Starting

- Keep Stage 1 compile-safe and testable on its own.
- Do not change `client.rs`, `client_tun.rs`, or `server.rs` behavior yet except for non-invasive imports if absolutely required.
- Unit tests are enough for this stage because protocol framing is pure logic and should be verified without standing up sockets.

### Task 1: Add Shared Module Skeleton

**Files:**
- Create: `src/shared/mod.rs`
- Create: `src/shared/errors.rs`
- Modify: `src/main.rs`
- Modify: `Cargo.toml`

- [ ] **Step 1: Write the failing dependency and module wiring check**

Create a temporary compile target by adding this import block expectation to your local notes before editing:

```rust
mod shared;

use crate::shared::errors::ClientError;
```

Expected failure before code changes:

- `file not found for module shared`
- unresolved crate `thiserror`

- [ ] **Step 2: Add the new dependency**

Edit `Cargo.toml` and add:

```toml
[dependencies]
tokio = { version = "1", features = ["full"] }
chacha20poly1305 = "0.10"
rand = "0.8"
tokio-util = { version = "0.7", features = ["codec","compat"] }
bytes = "1.0"
futures = "0.3"
tokio-rustls = "0.24"
rustls = "0.21"
rustls-pemfile = "1.0"
yamux = "0.10"
tun = { version = "0.6", features = ["async"] }
smoltcp = { version = "0.10", default-features = false, features = ["std", "medium-ip", "proto-ipv4", "socket-tcp", "socket-udp"] }
etherparse = "0.13"
thiserror = "1.0"
```

- [ ] **Step 3: Create `src/shared/mod.rs`**

Write:

```rust
pub mod errors;
pub mod relay_protocol;
pub mod target;
```

- [ ] **Step 4: Create `src/shared/errors.rs`**

Write:

```rust
use thiserror::Error;

/// Shared client-side errors used by protocol framing and runtime adapters.
#[derive(Debug, Error)]
pub enum ClientError {
    #[error("invalid target address: {0}")]
    InvalidTarget(String),
    #[error("invalid relay request: {0}")]
    InvalidRelayRequest(String),
    #[error("protocol frame too short")]
    FrameTooShort,
    #[error("unsupported transport kind: {0}")]
    UnsupportedTransport(u8),
    #[error("utf8 decode error")]
    Utf8,
}
```

- [ ] **Step 5: Register the shared module in `src/main.rs`**

Update the module declarations at the top of `src/main.rs` to:

```rust
mod client;
mod client_tun;
mod device;
mod server;
mod shared;
```

- [ ] **Step 6: Run compile check**

Run:

```bash
cargo check
```

Expected:

- `Finished` or `Checking mini_vpn`
- no module resolution error for `shared`

- [ ] **Step 7: Commit**

Run:

```bash
git add Cargo.toml src/main.rs src/shared/mod.rs src/shared/errors.rs
git commit -m "feat: add shared module skeleton"
```

### Task 2: Add Structured Target Address Model

**Files:**
- Create: `src/shared/target.rs`
- Test: `src/shared/target.rs`

- [ ] **Step 1: Write the failing tests**

Create `src/shared/target.rs` with the following tests first:

```rust
#[cfg(test)]
mod tests {
    use super::TargetAddr;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    #[test]
    fn parses_ipv4_target() {
        let actual = TargetAddr::parse("127.0.0.1:7897").unwrap();
        let expected = TargetAddr::IpPort(SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 7897));
        assert_eq!(actual, expected);
    }

    #[test]
    fn parses_domain_target() {
        let actual = TargetAddr::parse("www.figma.com:443").unwrap();
        let expected = TargetAddr::DomainPort {
            host: "www.figma.com".to_string(),
            port: 443,
        };
        assert_eq!(actual, expected);
    }

    #[test]
    fn serializes_domain_target() {
        let target = TargetAddr::DomainPort {
            host: "mtalk.google.com".to_string(),
            port: 5228,
        };
        assert_eq!(target.as_string(), "mtalk.google.com:5228");
    }

    #[test]
    fn rejects_missing_port() {
        let err = TargetAddr::parse("www.figma.com").unwrap_err();
        assert!(err.to_string().contains("invalid target address"));
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
cargo test parses_ipv4_target -- --nocapture
```

Expected:

- FAIL because `TargetAddr` does not exist yet

- [ ] **Step 3: Write the minimal implementation**

Add the implementation above the test module:

```rust
use crate::shared::errors::ClientError;
use std::net::SocketAddr;

/// Structured target address shared by direct proxy mode and TUN mode.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TargetAddr {
    /// Concrete IP endpoint.
    IpPort(SocketAddr),
    /// Domain name with port, preserved before remote resolution.
    DomainPort { host: String, port: u16 },
}

impl TargetAddr {
    /// Parse `host:port` into a structured target model.
    pub fn parse(input: &str) -> Result<Self, ClientError> {
        if let Ok(addr) = input.parse::<SocketAddr>() {
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

    /// Convert the structured target back to a stable `host:port` string.
    pub fn as_string(&self) -> String {
        match self {
            Self::IpPort(addr) => addr.to_string(),
            Self::DomainPort { host, port } => format!("{host}:{port}"),
        }
    }
}
```

- [ ] **Step 4: Run the target tests**

Run:

```bash
cargo test target::tests -- --nocapture
```

Expected:

- all 4 target tests PASS

- [ ] **Step 5: Commit**

Run:

```bash
git add src/shared/target.rs
git commit -m "feat: add shared target address model"
```

### Task 3: Add Relay Request Framing

**Files:**
- Create: `src/shared/relay_protocol.rs`
- Test: `src/shared/relay_protocol.rs`

- [ ] **Step 1: Write the failing tests**

Create `src/shared/relay_protocol.rs` with the following tests first:

```rust
#[cfg(test)]
mod tests {
    use super::{RelayRequest, TransportKind, FAKE_HEADER};
    use crate::shared::target::TargetAddr;

    #[test]
    fn fake_header_is_stable() {
        assert_eq!(FAKE_HEADER, b"GET / HTTP/1.1\r\nHost: www.bing.com\r\n\r\n");
    }

    #[test]
    fn tcp_request_round_trip() {
        let request = RelayRequest::Tcp {
            target: TargetAddr::parse("34.107.238.235:443").unwrap(),
        };
        let encoded = request.encode();
        let decoded = RelayRequest::decode(&encoded).unwrap();
        assert_eq!(decoded, request);
    }

    #[test]
    fn udp_request_round_trip_with_target() {
        let request = RelayRequest::Udp {
            target: Some(TargetAddr::parse("mtalk.google.com:5228").unwrap()),
        };
        let encoded = request.encode();
        let decoded = RelayRequest::decode(&encoded).unwrap();
        assert_eq!(decoded, request);
    }

    #[test]
    fn udp_request_round_trip_without_target() {
        let request = RelayRequest::Udp { target: None };
        let encoded = request.encode();
        let decoded = RelayRequest::decode(&encoded).unwrap();
        assert_eq!(decoded, request);
    }

    #[test]
    fn rejects_empty_frame() {
        let err = RelayRequest::decode(&[]).unwrap_err();
        assert!(err.to_string().contains("frame too short"));
    }

    #[test]
    fn transport_kind_byte_values_are_stable() {
        assert_eq!(TransportKind::Tcp.as_byte(), 1);
        assert_eq!(TransportKind::Udp.as_byte(), 2);
    }
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run:

```bash
cargo test relay_protocol::tests -- --nocapture
```

Expected:

- FAIL because `RelayRequest`, `TransportKind`, and `FAKE_HEADER` do not exist yet

- [ ] **Step 3: Write the minimal implementation**

Add the implementation above the tests:

```rust
use crate::shared::errors::ClientError;
use crate::shared::target::TargetAddr;

/// Stable camouflage header sent before every relay request.
pub const FAKE_HEADER: &[u8; 38] = b"GET / HTTP/1.1\r\nHost: www.bing.com\r\n\r\n";

/// Shared transport kind used by direct proxy mode and TUN mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportKind {
    Tcp,
    Udp,
}

impl TransportKind {
    /// Serialize transport kind into a stable protocol byte.
    pub fn as_byte(self) -> u8 {
        match self {
            Self::Tcp => 1,
            Self::Udp => 2,
        }
    }

    /// Parse transport kind from the stable protocol byte.
    pub fn from_byte(byte: u8) -> Result<Self, ClientError> {
        match byte {
            1 => Ok(Self::Tcp),
            2 => Ok(Self::Udp),
            other => Err(ClientError::UnsupportedTransport(other)),
        }
    }
}

/// Shared relay request envelope.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RelayRequest {
    Tcp { target: TargetAddr },
    Udp { target: Option<TargetAddr> },
}

impl RelayRequest {
    /// Encode the request into a compact frame.
    pub fn encode(&self) -> Vec<u8> {
        match self {
            Self::Tcp { target } => {
                let mut out = vec![TransportKind::Tcp.as_byte()];
                out.extend_from_slice(target.as_string().as_bytes());
                out
            }
            Self::Udp { target } => {
                let mut out = vec![TransportKind::Udp.as_byte()];
                if let Some(target) = target {
                    out.extend_from_slice(target.as_string().as_bytes());
                }
                out
            }
        }
    }

    /// Decode the compact frame into a relay request.
    pub fn decode(frame: &[u8]) -> Result<Self, ClientError> {
        let (&kind, rest) = frame.split_first().ok_or(ClientError::FrameTooShort)?;
        match TransportKind::from_byte(kind)? {
            TransportKind::Tcp => {
                let text = std::str::from_utf8(rest).map_err(|_| ClientError::Utf8)?;
                let target = TargetAddr::parse(text)?;
                Ok(Self::Tcp { target })
            }
            TransportKind::Udp => {
                if rest.is_empty() {
                    return Ok(Self::Udp { target: None });
                }
                let text = std::str::from_utf8(rest).map_err(|_| ClientError::Utf8)?;
                let target = TargetAddr::parse(text)?;
                Ok(Self::Udp { target: Some(target) })
            }
        }
    }
}
```

- [ ] **Step 4: Run protocol tests**

Run:

```bash
cargo test relay_protocol::tests -- --nocapture
```

Expected:

- all relay protocol tests PASS

- [ ] **Step 5: Run full test suite**

Run:

```bash
cargo test
```

Expected:

- all existing tests plus new shared module tests PASS

- [ ] **Step 6: Commit**

Run:

```bash
git add src/shared/relay_protocol.rs
git commit -m "feat: add shared relay request framing"
```

### Task 4: Tighten Shared Error Coverage

**Files:**
- Modify: `src/shared/errors.rs`
- Test: `src/shared/relay_protocol.rs`

- [ ] **Step 1: Add one failing test for invalid transport**

Append this test to `src/shared/relay_protocol.rs`:

```rust
#[test]
fn rejects_unknown_transport_byte() {
    let err = RelayRequest::decode(&[9, b'x']).unwrap_err();
    assert!(err.to_string().contains("unsupported transport kind: 9"));
}
```

- [ ] **Step 2: Run the specific test**

Run:

```bash
cargo test rejects_unknown_transport_byte -- --nocapture
```

Expected:

- PASS if Task 3 implementation already maps unknown transport correctly
- FAIL only if error formatting does not match

- [ ] **Step 3: If needed, adjust the error definitions**

`src/shared/errors.rs` should remain:

```rust
use thiserror::Error;

/// Shared client-side errors used by protocol framing and runtime adapters.
#[derive(Debug, Error)]
pub enum ClientError {
    #[error("invalid target address: {0}")]
    InvalidTarget(String),
    #[error("invalid relay request: {0}")]
    InvalidRelayRequest(String),
    #[error("protocol frame too short")]
    FrameTooShort,
    #[error("unsupported transport kind: {0}")]
    UnsupportedTransport(u8),
    #[error("utf8 decode error")]
    Utf8,
}
```

- [ ] **Step 4: Run targeted protocol tests again**

Run:

```bash
cargo test relay_protocol::tests -- --nocapture
```

Expected:

- PASS

- [ ] **Step 5: Commit**

Run:

```bash
git add src/shared/errors.rs src/shared/relay_protocol.rs
git commit -m "test: cover relay protocol error cases"
```

### Task 5: Write the Stage 1 Teaching Document

**Files:**
- Document: `docs/tech/01-shared-models-and-relay-protocol.md`

- [ ] **Step 1: Write the teaching document**

Create `docs/tech/01-shared-models-and-relay-protocol.md` with this content:

```md
# 01 Shared Models And Relay Protocol

## Why This Stage Exists

Before this stage, the project mixed target parsing, fake-header writing, and transport branching directly inside large runtime files. That made the code hard to extend for arbitrary ports, optional TUN mode, and shared TCP/UDP behavior.

This stage extracts the first shared building blocks:

- `TargetAddr`
- `TransportKind`
- `RelayRequest`
- `ClientError`
- `FAKE_HEADER`

## What Changed

- Added `src/shared/mod.rs`
- Added `src/shared/target.rs`
- Added `src/shared/relay_protocol.rs`
- Added `src/shared/errors.rs`
- Registered the shared module from `src/main.rs`
- Added unit tests for target parsing and relay request framing

## Core Design

### `TargetAddr`

`TargetAddr` separates address structure from runtime string literals.
It supports:

- `IpPort(SocketAddr)`
- `DomainPort { host, port }`

This lets later stages pass targets around as typed data instead of rebuilding strings in every branch.

### `RelayRequest`

`RelayRequest` becomes the shared request envelope for both direct proxy mode and TUN mode:

- `Tcp { target }`
- `Udp { target }`

That means client-side adapters can ask for a remote relay in one consistent way, even if their local ingress path differs.

### `FAKE_HEADER`

The camouflage header is now defined once.
This avoids branch drift where one adapter sends it and another forgets to.

## Data Flow Impact

Stage 1 does not yet change runtime behavior.
Instead, it creates a stable center that later stages will call into:

`client.rs` / `client_tun.rs` -> `RelayRequest` -> shared handshake -> `server.rs`

## Trade-Offs

- We intentionally keep Stage 1 small and pure-logic heavy
- We do not yet rewrite the adapters
- We prefer unit tests here because framing and parsing should be validated without network setup

## How To Verify

Run:

```bash
cargo test
```

Expected:

- target parsing tests pass
- relay protocol encoding/decoding tests pass

## What Comes Next

Stage 2 will refactor `client.rs` to replace ad-hoc target handling and ad-hoc handshake writes with the shared protocol introduced here.
```

- [ ] **Step 2: Run formatting and tests**

Run:

```bash
cargo test
```

Expected:

- PASS

- [ ] **Step 3: Commit**

Run:

```bash
git add docs/tech/01-shared-models-and-relay-protocol.md
git commit -m "docs: add stage 1 teaching note"
```

### Task 6: Final Verification

**Files:**
- Modify: `Cargo.toml`
- Modify: `src/main.rs`
- Create: `src/shared/mod.rs`
- Create: `src/shared/errors.rs`
- Create: `src/shared/target.rs`
- Create: `src/shared/relay_protocol.rs`
- Document: `docs/tech/01-shared-models-and-relay-protocol.md`

- [ ] **Step 1: Run final compile verification**

Run:

```bash
cargo check
```

Expected:

- PASS

- [ ] **Step 2: Run final test verification**

Run:

```bash
cargo test
```

Expected:

- PASS

- [ ] **Step 3: Run documentation check**

Verify that both documents exist:

```bash
ls docs/tech
```

Expected output should include:

```text
01-shared-models-and-relay-protocol.md
2026-05-07-mini-vpn-relay-runtime-spec.md
2026-05-07-stage-1-shared-models-and-relay-protocol-plan.md
```

- [ ] **Step 4: Create the final Stage 1 checkpoint commit**

Run:

```bash
git add Cargo.toml src/main.rs src/shared docs/tech
git commit -m "feat: complete stage 1 shared relay protocol foundation"
```

## Self-Review

### Spec Coverage

- Shared target model: covered in Task 2
- Shared relay request framing: covered in Task 3
- Shared error model: covered in Task 1 and Task 4
- Stage-level documentation requirement: covered in Task 5
- Compile-safe incremental delivery: covered by compile/test steps in every task

### Placeholder Scan

- No `TODO`
- No `TBD`
- No unspecified test commands
- No implicit file paths

### Type Consistency

- `ClientError` is defined in `src/shared/errors.rs`
- `TargetAddr` is defined in `src/shared/target.rs`
- `TransportKind`, `RelayRequest`, and `FAKE_HEADER` are defined in `src/shared/relay_protocol.rs`
- Later tasks reference the same type names consistently

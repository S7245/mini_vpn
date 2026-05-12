# Stage 6 Spec: TUN Upstream Minimal Configuration

## Overview

Stage 6 extends the Stage 5 TUN runtime configuration work by removing two remaining hardcoded upstream connection values from `src/client_tun.rs`:

- `server_addr`
- `tls_sni`

The goal is to make the TUN client configurable for different upstream endpoints without changing source code, while keeping the scope intentionally small.

This stage does not change the relay protocol, listener-pool behavior, or the direct client path.

## Preconditions

This stage builds on top of Stage 5:

- TUN listener configuration is already runtime-driven
- `local_port`, `target_addr`, and `pool_size` are configurable
- the listener pool runtime is stable
- local end-to-end testing has already succeeded in a privileged environment

Additional requirement carried forward:

- comments in new or updated code should remain English-led, with short Chinese key-point notes where useful

## Goals

### Functional Goals

- Remove hardcoded upstream address `127.0.0.1:8081`
- Remove hardcoded TLS SNI `localhost`
- Add startup-time configuration for:
  - `server_addr`
  - `tls_sni`
- Preserve current default behavior when no override is provided

### Safety Goals

- Validate explicit upstream configuration before entering the runtime hot path
- Reject malformed `server_addr`
- Reject malformed `tls_sni`
- Avoid silent fallback when the user explicitly provides invalid values

### Maintainability Goals

- Separate local interception config from upstream tunnel config
- Keep the config boundary clear enough to support later `cert_path` expansion
- Keep comments English-led with Chinese key-point notes

### Validation Goals

- Pass `cargo test`
- Pass `cargo check`
- Pass `cargo clippy --all-targets --all-features -- -D warnings`
- Pass `cargo doc --no-deps`
- Preserve current local-default startup behavior

## Non-Goals

This stage does not include:

- configuring certificate path
- unifying `client-direct` with `client-tun`
- redesigning CLI parsing
- changing shared handshake or relay wire format
- adding reconnect policy or upstream failover

## Architecture

### Configuration Structure

Stage 5 used one flat `TunRuntimeConfig`. Stage 6 should split it into local and upstream sub-configs:

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

This split matches the two physical roles in the runtime:

- `listener` describes how the TUN-side interception surface behaves
- `upstream` describes how the client reaches the remote TLS/Yamux server

### Runtime Flow

```text
start_tun_proxy()
-> TunRuntimeConfig::from_env()
-> runtime_config.listener.listener_spec()
-> runtime_config.listener.target_addr.clone()
-> runtime_config.upstream.server_addr
-> runtime_config.upstream.tls_sni
-> TCP connect + TLS handshake + Yamux setup
-> Stage 5 runtime loop
```

This keeps the startup logic explicit and avoids mixing unrelated configuration categories in a flat structure.

## Configuration Model

### Recommended Shapes

```rust
#[derive(Debug, Clone)]
struct TunRuntimeConfig {
    listener: TunListenerConfig,
    upstream: TunUpstreamConfig,
}

#[derive(Debug, Clone)]
struct TunListenerConfig {
    local_port: u16,
    target_addr: TargetAddr,
    pool_size: usize,
}

#[derive(Debug, Clone)]
struct TunUpstreamConfig {
    server_addr: String,
    tls_sni: String,
}
```

### Environment Variables

Stage 6 adds:

- `MINI_VPN_TUN_SERVER_ADDR`
- `MINI_VPN_TUN_TLS_SNI`

Stage 5 variables remain unchanged:

- `MINI_VPN_TUN_LOCAL_PORT`
- `MINI_VPN_TUN_TARGET_ADDR`
- `MINI_VPN_TUN_POOL_SIZE`

### Defaults

The default upstream values should preserve current behavior:

- `MINI_VPN_TUN_SERVER_ADDR` -> `127.0.0.1:8081`
- `MINI_VPN_TUN_TLS_SNI` -> `localhost`

### Validation Rules

- `server_addr` must be syntactically valid for `TcpStream::connect`
- first version should validate as `std::net::SocketAddr`
- `tls_sni` must be accepted by `ServerName::try_from(...)`

If a variable is absent, the default applies.

If a variable is explicitly present but malformed, startup should fail clearly.

## Error Handling

This stage should continue using the current startup error style:

- parse/validation failure -> `println!(...)` and `return`
- connection failure -> existing logging path remains intact

The error typing should stay minimal. If the current `ClientError` enum can cleanly express these failures, reuse it. If not, adding narrow variants such as `InvalidServerAddr` and `InvalidTlsSni` is acceptable.

This stage should not trigger a broader error-handling refactor.

## File Impact

### Primary Code File

- `src/client_tun.rs`

Expected changes:

- split `TunRuntimeConfig`
- add `TunUpstreamConfig`
- update `from_env()` and source parsing
- replace hardcoded `ServerName::try_from("localhost")`
- replace hardcoded `TcpStream::connect("127.0.0.1:8081")`
- extend startup log to include upstream values

### Documentation

- add a new Stage 6 teaching note under `/docs/tech/`

## Implementation Outline

### Step 1

Refactor `TunRuntimeConfig` into:

- `TunListenerConfig`
- `TunUpstreamConfig`
- wrapper `TunRuntimeConfig`

### Step 2

Add source/default parsing for:

- `server_addr`
- `tls_sni`

### Step 3

Replace the current hardcoded upstream values in `start_tun_proxy()`

### Step 4

Add unit tests for:

- default upstream values
- valid overrides
- invalid `server_addr`
- invalid `tls_sni`

### Step 5

Run full validation and perform a runtime smoke check where possible

## Testing Matrix

### Unit Tests

- default listener + upstream config path
- valid upstream override path
- invalid `MINI_VPN_TUN_SERVER_ADDR`
- invalid `MINI_VPN_TUN_TLS_SNI`

### Static Checks

- `cargo test`
- `cargo check`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo doc --no-deps`

### Runtime Checks

- default startup log includes:
  - `server_addr=127.0.0.1:8081`
  - `tls_sni=localhost`
- override startup log includes custom values
- if TUN permission is unavailable, config logging should still happen before the permission boundary

## Risks

### Risk 1: Mixed Config Responsibilities Return

Mitigation:

- split local and upstream config explicitly
- keep listener and upstream fields in separate structs

### Risk 2: Weak Upstream Validation

Mitigation:

- validate `server_addr` at startup
- validate `tls_sni` at startup
- reject explicit malformed input

### Risk 3: Scope Creep Into Full Client Config Unification

Mitigation:

- do not modify `client.rs`
- do not add `cert_path` in this stage
- keep changes inside `client_tun.rs` plus docs/tests

## Success Criteria

Stage 6 is complete when all of the following are true:

- `client_tun.rs` no longer hardcodes the upstream server address
- `client_tun.rs` no longer hardcodes the TLS SNI
- both values are configurable at startup
- defaults preserve current behavior
- malformed explicit upstream config fails clearly
- comments continue following the English-led + Chinese-key-point style
- all validation commands pass

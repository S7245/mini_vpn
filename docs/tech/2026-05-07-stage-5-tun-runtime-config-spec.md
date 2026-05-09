# Stage 5 Spec: TUN Runtime Minimal Configuration

## Overview

Stage 5 introduces a small runtime configuration layer for the TUN client path.

The current Stage 4 runtime works, but three key values are still hardcoded inside `src/client_tun.rs`:

- local listener port
- default target address
- listener pool size

This stage replaces those hardcoded values with a focused `TunRuntimeConfig` that reads validated runtime inputs while preserving Stage 4 behavior as the default path.

The scope stays intentionally narrow: this stage only configures the TUN runtime, and only for `local_port`, `target_addr`, and `pool_size`.

## Preconditions

This stage builds on top of the already completed Stage 4 runtime:

- the real 4-slot listener pool is active
- per-handle lifecycle isolation is in place
- the shared relay protocol remains unchanged
- local end-to-end testing has already succeeded on a machine with TUN permissions

Additional requirement carried into Stage 5:

- comments in new or updated code should be English-led, with short Chinese key-point notes where helpful

## Goals

### Functional Goals

- Add a `TunRuntimeConfig` to `client_tun.rs`
- Read runtime configuration for:
  - `local_port`
  - `target_addr`
  - `pool_size`
- Preserve current Stage 4 behavior when environment variables are absent
- Keep the rest of the TUN runtime unchanged as much as possible

### Safety Goals

- Validate configuration before the runtime enters the hot path
- Reject invalid `pool_size`, invalid `local_port`, and invalid `target_addr`
- Avoid silent fallback on malformed explicit input

### Maintainability Goals

- Separate configuration parsing from runtime assembly
- Keep `ListenerSpec` focused on listener-pool structure rather than becoming a generic config dump
- Keep comments English-led with Chinese key-point reinforcement

### Validation Goals

- Pass `cargo test`
- Pass `cargo check`
- Pass `cargo clippy --all-targets --all-features -- -D warnings`
- Pass `cargo doc --no-deps`
- Verify default config still behaves like Stage 4

## Non-Goals

This stage does not include:

- unifying `client-direct` and `client-tun` under one shared runtime config layer
- adding `server_addr` or `tls_sni` configuration
- redesigning CLI argument parsing
- introducing a full config file format
- changing shared relay protocol formats

## Architecture

### Runtime Structure

```text
start_tun_proxy()
├── TunRuntimeConfig::from_env()
├── derive ListenerSpec from config
├── clone TargetAddr from config
├── build_listener_pool()
└── continue Stage 4 runtime unchanged
```

### Design Boundary

`TunRuntimeConfig` should own:

- where config comes from
- default values
- validation

`ListenerSpec` should continue to own:

- the structural description of the listener pool

`TargetAddr` should continue to own:

- target parsing and normalization

This avoids mixing runtime input parsing with the hot-path TCP/TUN orchestration.

## Configuration Model

### `TunRuntimeConfig`

Recommended shape:

```rust
struct TunRuntimeConfig {
    local_port: u16,
    target_addr: TargetAddr,
    pool_size: usize,
}
```

Recommended helpers:

```rust
impl TunRuntimeConfig {
    fn from_env() -> Result<Self, ClientError>;
    fn listener_spec(&self) -> ListenerSpec;
}
```

### Environment Variables

Stage 5 should support these inputs:

- `MINI_VPN_TUN_LOCAL_PORT`
- `MINI_VPN_TUN_TARGET_ADDR`
- `MINI_VPN_TUN_POOL_SIZE`

### Defaults

If variables are absent, the runtime should keep current behavior:

- `MINI_VPN_TUN_LOCAL_PORT` → `80`
- `MINI_VPN_TUN_TARGET_ADDR` → `httpbin.org:80`
- `MINI_VPN_TUN_POOL_SIZE` → `4`

### Validation Rules

- `local_port` must parse as `u16`
- `target_addr` must parse through `TargetAddr::parse()`
- `pool_size` must parse as `usize`
- `pool_size` must be at least `1`

If a variable is explicitly provided but invalid, startup should fail clearly rather than silently reverting to a default.

## Data Flow

### Before Stage 5

```text
start_tun_proxy()
-> read hardcoded constants
-> build ListenerSpec
-> parse hardcoded target
-> continue runtime
```

### After Stage 5

```text
start_tun_proxy()
-> TunRuntimeConfig::from_env()
-> config.listener_spec()
-> config.target_addr.clone()
-> continue Stage 4 runtime
```

## Implementation Plan For This Stage

### Step 1: Add Stage 4 Acceptance Summary

Create a short Stage 4 closure document in `/docs/tech/` that records:

- static validation status
- local runtime validation success
- completed commits
- remaining known gaps after Stage 4

This document is the formal close-out for milestone A.

### Step 2: Add `TunRuntimeConfig`

Inside `src/client_tun.rs`, add:

- the struct definition
- English-led comments with Chinese key-point notes
- `from_env()`
- `listener_spec()`

The code should remain local to `client_tun.rs` in this stage.

### Step 3: Replace Hardcoded TUN Runtime Constants

Use `TunRuntimeConfig` to derive:

- local listener port
- target address
- pool size

Replace direct runtime dependence on:

- `DEFAULT_TUN_LISTEN_PORT`
- `DEFAULT_TUN_TARGET`
- `DEFAULT_TUN_POOL_SIZE`

The constants may remain as default-value sources if that keeps the code simpler, but they should no longer act as the direct runtime configuration path.

### Step 4: Add Unit Tests For Config Parsing

Add unit tests that prove:

- default config loads when env vars are absent
- invalid port input fails
- invalid pool size input fails
- invalid target input fails

If tests need environment isolation, they should guard and restore process env carefully.

### Step 5: Validate Runtime Compatibility

Run:

- `cargo test`
- `cargo check`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo doc --no-deps`

If possible, also verify:

- default behavior still starts with the same TUN runtime settings as Stage 4
- overriding `MINI_VPN_TUN_POOL_SIZE=2` changes startup log output accordingly

## Testing Matrix

### Unit Tests

- default config path
- invalid `MINI_VPN_TUN_LOCAL_PORT`
- invalid `MINI_VPN_TUN_TARGET_ADDR`
- invalid `MINI_VPN_TUN_POOL_SIZE`
- valid override path

### Static Checks

- `cargo test`
- `cargo check`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo doc --no-deps`

### Runtime Checks

- no env vars: same behavior as Stage 4
- custom `MINI_VPN_TUN_POOL_SIZE`: slot count changes in startup log
- custom `MINI_VPN_TUN_TARGET_ADDR`: runtime uses alternate target

## Risks

### Risk 1: Config Parsing Bleeds Into Hot Path

Mitigation:

- keep parsing in `TunRuntimeConfig::from_env()`
- derive `ListenerSpec` once at startup only

### Risk 2: Invalid Explicit Input Silently Falls Back

Mitigation:

- only use defaults when the variable is absent
- if the variable is present but malformed, return an error

### Risk 3: Tests Interfere Through Shared Process Environment

Mitigation:

- keep env-manipulating tests serialized or carefully restore environment state
- minimize the number of env-touching tests

## Success Criteria

Stage 5 is complete when all of the following are true:

- `client_tun.rs` has a real `TunRuntimeConfig`
- `local_port`, `target_addr`, and `pool_size` are no longer hardcoded as the direct runtime source
- invalid explicit config fails clearly
- default config preserves Stage 4 behavior
- comments follow the English-led with Chinese key-point style
- Stage 4 acceptance summary is written
- all validation commands pass

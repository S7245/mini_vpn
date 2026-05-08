# Stage 4 Spec: Listener Pool Activation

## Overview

Stage 4 activates the TUN-side listener pool for real.

The previous stage introduced a pool-friendly skeleton in `client_tun.rs`, but runtime behavior still uses a single logical listener slot. This stage turns that skeleton into a real 4-slot TCP listener pool so the TUN path can accept repeated short-lived connections and begin handling light concurrency without collapsing back into the original "single hotel room" limitation.

This stage does not redesign the shared relay protocol and does not introduce UDP pool handling yet. It focuses on making the TCP-over-TUN path operational, observable, and maintainable.

## Preconditions

This stage inherits the project-wide design constraints already agreed on:

- Target address must not be hardcoded as `:80` in the architecture model, even though the current TUN runtime still uses a temporary default target during this stage.
- The product model must support `TCP`, `UDP`, and optional `TUN`.
- `TUN` is an optional runtime mode, not a mandatory base layer.
- Shared relay handshake stays unified across `DirectProxy` and `TunGateway`.

Stage 4 narrows scope further with these local constraints:

- Listener pool size is activated at `4`.
- The work is limited to TCP-over-TUN pool activation.
- The current default TUN target remains a centralized constant for now.
- Server/client shared relay protocol remains unchanged.

## Goals

### Functional Goals

- Activate a real listener pool with `4` TCP listener sockets on the TUN path.
- Ensure each `SocketHandle` owns an independent `SocketCtx`.
- Ensure EOF, close, and re-listen behavior only affects the corresponding slot.
- Preserve compatibility with the existing shared relay protocol.

### Performance Goals

- Support repeated sequential `curl 10.0.0.2:80` requests without the "first request succeeds, second fails" behavior.
- Support light concurrency across multiple handles.
- Keep the design ready for later buffer tuning and deeper zero-copy optimization.

### Safety Goals

- Add richer EN/CN comments on enums, structs, important functions, and important state variables.
- Reduce unexplained hot-path `unwrap()` usage where practical during this stage.
- Make state transitions easier to reason about through code structure and logs.

### Validation Goals

- Pass `cargo test`
- Pass `cargo check`
- Pass `cargo clippy --all-targets --all-features -- -D warnings`
- Pass `cargo doc --no-deps`
- If local permissions and environment allow it, run `server -> client-tun -> curl` end-to-end checks

## Non-Goals

This stage intentionally does not include:

- UDP listener pool activation for TUN mode
- Full runtime configuration system for pool size / target / ports
- Full removal of all `unwrap()` calls from `client_tun.rs`
- Shared relay protocol redesign
- QUIC / HTTP3 / alternate transport work

## Architecture

### Crate Responsibility View

```text
mini_vpn
├── src/main.rs
│   └── selects runtime mode
├── src/client.rs
│   └── direct proxy adapter using shared relay protocol
├── src/client_tun.rs
│   └── TUN adapter with smoltcp listener pool
├── src/server.rs
│   └── relay server using shared request parsing
├── src/device.rs
│   └── TUN device wrapper
└── src/shared/
    ├── errors.rs
    ├── target.rs
    ├── relay_protocol.rs
    └── tunnel.rs
```

### Runtime Structure Inside `client_tun.rs`

```text
start_tun_proxy()
├── build ListenerSpec(local_port=80, pool_size=4)
├── build ListenerPool(handles=[h1, h2, h3, h4])
├── build SocketCtx for each handle
├── create shared TLS + Yamux control channel
└── event loop
    ├── poll TUN ingress
    ├── flush egress
    ├── process per-handle listener activity
    └── process per-handle remote payload return
```

## Core Data Structures

### `ListenerSpec`

Purpose:

- Defines how many listener slots the TUN runtime should create for a given local TCP port.

Stage 4 shape:

- `local_port: u16`
- `pool_size: usize`

Stage 4 decision:

- Default value is `local_port = 80`
- Activated value is `pool_size = 4`

### `ListenerPool`

Purpose:

- Holds the set of `SocketHandle` values that represent active smoltcp listener slots.

Stage 4 behavior:

- Allocates 4 distinct `TcpSocket` values
- Calls `listen(local_port)` on each socket
- Stores all generated handles

### `SocketState`

Purpose:

- Makes connection lifecycle explicit instead of implicit.

Retained states:

- `Listening`
- `OpeningRemote`
- `Relaying`
- `Closing`
- `Rearming`

Stage 4 use:

- State transitions are updated per-handle.
- Logs and comments should explain why a handle is in a given state.

### `SocketCtx`

Purpose:

- Owns per-handle runtime metadata so listener slots stay isolated.

Stage 4 required fields:

- `local_port`
- `state`
- `target`
- `uplink_tx`

Stage 4 behavior:

- One `SocketCtx` per `SocketHandle`
- One remote relay task per active handle
- One close/rearm path per active handle

## Data Flow

### Local Ingress

```text
OS -> TUN -> VirtualTunDevice -> smoltcp iface.poll()
-> iterate listener handles
-> extract per-handle payload
-> if first payload:
      build RelayRequest::Tcp
      open_remote_session()
      spawn relay task
   else:
      forward payload via uplink_tx
```

### Remote Return Path

```text
remote relay task
-> read Yamux substream
-> send (handle, payload) to global mailbox
-> main loop receives payload
-> handle_remote_payload()
-> write into matching TcpSocket
-> flush packet to TUN device
```

### EOF / Rearm Path

```text
remote EOF for handle X
-> send (X, empty payload)
-> main loop dispatches to handle X
-> mark Closing
-> abort socket X
-> clear uplink_tx
-> listen(local_port) again on socket X
-> mark Listening
```

## Implementation Plan For This Stage

### Step 1: Strengthen Comments And Runtime Intent

Add richer EN/CN comments for:

- `ListenerSpec`
- `ListenerPool`
- `SocketState`
- `SocketCtx`
- `build_listener_socket()`
- `rearm_socket()`
- `process_listener_activity()`
- `handle_local_payload()`
- `handle_remote_payload()`
- `spawn_remote_relay()`

Comment style requirement:

- Explain intent and invariant, not just syntax.
- Example: explain why a `SocketCtx` exists, not just that it stores state.

### Step 2: Activate Real Pool Allocation

Change listener creation from:

- one `TcpSocket`

to:

- a loop that creates `4` `TcpSocket` listener slots
- one `SocketCtx` per handle
- one shared `ListenerPool` containing all handles

### Step 3: Preserve Per-Handle Isolation

Ensure the following helpers work per handle:

- `process_listener_activity()`
- `handle_local_payload()`
- `handle_remote_payload()`
- `rearm_socket()`

Acceptance rule:

- A close or EOF on one handle must not clear or rearm any other handle.

### Step 4: Improve Observability

Add per-handle logs at these moments:

- listener slot created
- first local payload observed
- remote session opened
- remote EOF received
- socket rearmed

These logs are needed for Stage 4 manual verification.

### Step 5: Validate Locally

Static validation:

- `cargo test`
- `cargo check`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo doc --no-deps`

Runtime validation if environment allows:

1. Start relay server
2. Start `client-tun`
3. Run `curl 10.0.0.2:80`
4. Run repeated `curl 10.0.0.2:80`
5. Attempt light parallel curl requests
6. Observe that multiple handles are created and rearmed independently

If a step requires local privileges or system-level TUN support, that requirement must be made explicit to the user instead of being silently skipped.

### Step 6: Write Teaching Note

Add a Stage 4 teaching document to `/docs/tech/` explaining:

- why skeleton-first was necessary
- what changed when the pool became real
- how per-handle isolation works
- how to test the result manually

## Testing Matrix

### Static Checks

- `cargo test`
- `cargo check`
- `cargo clippy --all-targets --all-features -- -D warnings`
- `cargo doc --no-deps`

### Functional Checks

- repeated sequential `curl 10.0.0.2:80`
- at least 4 sequential short-lived requests
- light parallel short-lived requests
- verify handle-specific rearm behavior in logs

### Regression Checks

- DirectProxy path still compiles
- Shared relay protocol tests remain green
- Server request parsing remains compatible

## Commit Strategy

If the implementation validates cleanly, use focused commits at key checkpoints:

1. Runtime activation commit
   - Example: `refactor(tun): activate 4-slot listener pool`
2. Documentation commit
   - Example: `docs(tun): add stage 4 listener pool note`

If the code and docs land together cleanly and the diff is still focused, a single combined commit is also acceptable.

## Risks

### Risk 1: Pool Activated But State Still Leaks Across Handles

Mitigation:

- Keep `SocketCtx` strictly per handle
- Keep rearm logic local to the handle that emitted EOF

### Risk 2: Manual Testing Blocked By TUN Privileges

Mitigation:

- Run all static checks regardless
- If runtime test needs elevated permissions or local network setup, provide exact commands and ask the user to run them

### Risk 3: Logs Become Too Noisy

Mitigation:

- Keep logs tied to state transition boundaries
- Prefer handle-oriented logs rather than dumping raw packet detail everywhere

## Success Criteria

Stage 4 is complete when all of the following are true:

- `client_tun.rs` creates a real 4-slot listener pool
- each handle owns its own `SocketCtx`
- repeated `curl 10.0.0.2:80` no longer fails after the first successful request
- per-handle rearm behavior is visible and isolated
- richer EN/CN comments are present on the key runtime structures and helpers
- required docs are written
- validation commands pass

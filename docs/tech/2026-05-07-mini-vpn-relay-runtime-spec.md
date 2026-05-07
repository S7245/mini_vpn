# Mini VPN Relay Runtime Spec

## Document Info

- Date: 2026-05-07
- Status: Draft for review
- Scope: Client runtime architecture, shared tunnel protocol, TUN optional mode, listener pooling, staged implementation and documentation workflow

## 1. Goals

This spec defines the next-stage architecture for the `mini_vpn` project.
The design must support:

- Direct proxy mode without TUN
- Optional TUN gateway mode
- TCP relay
- UDP relay
- Arbitrary target addresses and ports
- Configurable listener pooling for TUN mode
- Shared TLS + Yamux tunnel layer
- Step-by-step implementation with teaching documents under `/docs/tech/`

This spec intentionally focuses on architecture, component boundaries, state flow, and implementation sequencing before code changes.

## 2. Non-Negotiable Preconditions

The following conditions are treated as project-level design prerequisites.

### 2.1 Target Address Is Not Fixed

The runtime must not hardcode remote targets such as `httpbin.org:80` or assume local listener port `80` is the only case.

The design must support arbitrary targets, including but not limited to:

- `34.107.238.235:443`
- `www.figma.com:443`
- `mtalk.google.com:5228`
- `127.0.0.1:7897`

All target addresses must be modeled as structured runtime data instead of embedded string literals in hot paths.

### 2.2 Supported Traffic Types

The runtime must support:

- TCP relay
- UDP relay
- Optional TUN virtual network mode

### 2.3 TUN Is Optional

TUN is not a mandatory foundation for the client.

If the customer enables TUN mode:

- traffic enters through TUN
- packets are processed by `smoltcp`
- listener pooling is used for local TCP acceptance

If the customer does not enable TUN mode:

- the runtime must work as a direct TCP/UDP proxy
- the shared TLS + Yamux tunnel layer remains the same

### 2.4 Engineering Constraints

The implementation must follow these constraints:

- no duplicated handshake logic across branches
- no protocol-specific hardcoded target in lifecycle hot paths
- no `unwrap()` in connection lifecycle hot paths
- state transitions must be visible in logs
- TUN-specific code must remain isolated from direct proxy mode

## 3. Functional Objectives

### 3.1 Functional

- Support direct TCP relay to arbitrary `host:port`
- Support direct UDP relay to arbitrary `host:port` or UDP-associate style sessions
- Support optional TUN interception mode
- Support multiple listener sockets for the same local TCP port in TUN mode
- Support reusable lifecycle for each local socket slot
- Support shared remote tunnel setup for both direct mode and TUN mode

### 3.2 Performance

- Prioritize correctness of connection lifecycle before throughput tuning
- Avoid unnecessary copies where practical
- Keep listener pool size configurable
- Keep per-socket buffer size configurable instead of fixed large allocations

### 3.3 Safety

- Separate per-handle failures from global tunnel failures
- Prevent one failed relay task from tearing down unrelated sessions
- Replace panic-prone lifecycle code with typed errors and explicit recovery

## 4. Architecture Overview

### 4.1 Crate Responsibility Graph

```text
mini_vpn
├── main.rs
│   └── runtime entry and mode selection
├── client.rs
│   └── DirectProxy adapter
├── client_tun.rs
│   └── TunGateway adapter
├── server.rs
│   └── remote TLS + Yamux server and target forwarding
├── device.rs
│   └── tun::AsyncDevice <-> smoltcp bridge
└── shared/                (to be introduced)
    ├── target.rs
    ├── relay_protocol.rs
    ├── tunnel.rs
    └── errors.rs
```

### 4.2 Clean + Hexagonal Split

- Core domain:
  - target address model
  - relay request model
  - socket lifecycle state model
  - listener specification
- Adapters:
  - direct proxy adapter
  - TUN + smoltcp adapter
  - TLS + Yamux tunnel adapter
  - server-side target bridge adapter
- App assembly:
  - startup config
  - runtime mode selection
  - logging and metrics

## 5. Runtime Modes

### 5.1 Direct Proxy Mode

`DirectProxy` runs without TUN.

Entry sources:

- local TCP proxy entry
- local UDP proxy entry

This mode reuses the shared tunnel layer and target modeling but does not initialize TUN or `smoltcp`.

### 5.2 TUN Gateway Mode

`TunGateway` enables TUN and `smoltcp`.

Entry source:

- packets intercepted from TUN

Responsibilities:

- drive packet ingress and egress
- decode local sessions from `smoltcp`
- assign local TCP sessions to listener pool slots
- relay data through the shared tunnel layer

## 6. Shared Models

### 6.1 Runtime Mode

```rust
enum RuntimeMode {
    DirectProxy,
    TunGateway,
}
```

### 6.2 Transport Kind

```rust
enum TransportKind {
    Tcp,
    Udp,
}
```

### 6.3 Target Address

```rust
enum TargetAddr {
    IpPort(std::net::SocketAddr),
    DomainPort { host: String, port: u16 },
}
```

Rationale:

- avoids hardcoded string assembly in multiple branches
- supports both direct proxy mode and TUN mode
- allows future validation, logging, and serialization in one place

### 6.4 Listener Specification

```rust
struct ListenerSpec {
    local_port: u16,
    transport: TransportKind,
    pool_size: usize,
}
```

Rationale:

- local listener port is independent from remote target port
- allows `80`, `443`, `5228`, `7897`, or any configured port
- allows one port to have multiple listening sockets in TUN mode

### 6.5 Relay Request

```rust
enum RelayRequest {
    Tcp { target: TargetAddr },
    Udp { target: Option<TargetAddr> },
}
```

Rationale:

- creates one handshake format for all runtime modes
- avoids duplicated fake-header and target negotiation code

## 7. TUN Listener Pool Design

### 7.1 Listener Pool

```rust
struct ListenerPool {
    specs: Vec<ListenerSpec>,
    handles_by_listener: std::collections::HashMap<(TransportKind, u16), Vec<SocketHandle>>,
}
```

Responsibilities:

- initialize `N` sockets per configured local TCP listener
- call `listen(local_port)` during startup
- expose handles for polling and lifecycle management

### 7.2 Per-Handle Context

```rust
struct SocketCtx {
    transport: TransportKind,
    state: SocketState,
    local_port: u16,
    target: Option<TargetAddr>,
    uplink_tx: Option<tokio::sync::mpsc::Sender<Vec<u8>>>,
    remote_task_running: bool,
    bytes_from_local: u64,
    bytes_from_remote: u64,
}
```

### 7.3 Socket State

```rust
enum SocketState {
    Listening,
    OpeningRemote,
    Relaying,
    Closing,
    Rearming,
}
```

Rationale:

- each socket handle becomes an explicit session slot
- lifecycle becomes observable and testable
- pool expansion no longer requires rewriting business logic

## 8. Data Flow

### 8.1 Three-Bus Model

```text
Bus A: OS -> TUN -> VirtualTunDevice.wait_for_rx() -> smoltcp iface.poll()
Bus B: smoltcp sockets -> poll_socket_events() -> open/reuse remote session
Bus C: remote relay -> global_rx -> socket writeback -> flush_tx() -> OS
```

### 8.2 Direct Proxy Flow

```text
local TCP/UDP proxy request
-> parse target
-> build RelayRequest
-> open Yamux substream via shared handshake
-> relay bidirectionally
```

### 8.3 TUN Gateway TCP Flow

```text
OS packet
-> TUN
-> smoltcp poll
-> a listening handle becomes active
-> first payload extracted from that handle
-> RelayRequest::Tcp { target }
-> open remote Yamux session
-> relay local <-> remote
-> EOF or error
-> cleanup handle context
-> rearm socket
-> return to Listening
```

### 8.4 TUN Gateway UDP Flow

```text
OS UDP packet
-> TUN
-> smoltcp or UDP adapter extracts datagram/session info
-> RelayRequest::Udp { target }
-> remote relay task sends datagrams
-> response datagrams routed back
-> idle timeout or close
-> context cleanup
```

## 9. Single Handle Lifecycle

### 9.1 State Transition

```text
Listening
-> OpeningRemote
-> Relaying
-> Closing
-> Rearming
-> Listening
```

### 9.2 State Semantics

- `Listening`
  - socket slot is ready to accept a local TCP session
- `OpeningRemote`
  - the local session has produced enough intent to open a remote tunnel
- `Relaying`
  - local and remote data are flowing
- `Closing`
  - one side closed or errored, cleanup is in progress
- `Rearming`
  - socket is reset and returned to a listening state

### 9.3 Lifecycle Rule

Any per-handle failure must only retire or rearm the affected handle.
It must not take down unrelated handles or the whole tunnel runtime unless the shared TLS/Yamux connection itself has failed.

## 10. Shared Tunnel Handshake

### 10.1 Why This Must Be Shared

The current implementation duplicates:

- fake header transmission
- target address transmission
- protocol branching

This duplication has already caused branch drift.

### 10.2 Proposed Shared API

```rust
async fn open_remote_session(
    ctrl: &yamux::Control,
    request: RelayRequest,
) -> Result<RemoteSession, ClientError>
```

Responsibilities:

- open a new Yamux substream
- send the fake HTTP header used for camouflage
- serialize `RelayRequest`
- return a session object ready for relay

### 10.3 Server Expectations

The server must parse a single request envelope independent of whether the caller is direct proxy mode or TUN mode.

This prevents:

- mode-specific server branches
- duplicated parsing logic
- protocol mismatch between adapters

## 11. Error Handling

### 11.1 Layering

- `shared` and core-adjacent modules use `thiserror`
- app assembly and binaries may wrap with `anyhow`

### 11.2 Error Categories

```rust
enum ClientError {
    TunIo,
    SmoltcpPoll,
    SocketClosed,
    RemoteHandshake,
    RemoteRelay,
    ProtocolParse,
    RearmFailed,
}
```

### 11.3 Recovery Rules

- TUN read/write error:
  - fail the TUN runtime or enter reconnect strategy if later added
- single relay task error:
  - close and rearm only the affected handle
- Yamux substream error:
  - terminate the corresponding session only
- shared TLS/Yamux root connection failure:
  - fail the runtime or trigger reconnect logic in a future milestone

## 12. Buffering and Performance Notes

- Listener pool size must be configurable
- Socket buffer size must be configurable
- Do not keep all sockets at `64 KiB` buffers by default unless profiling justifies it
- Prefer `Bytes` or equivalent shared buffer types later where it reduces copies
- Zero-copy improvements are secondary to state-machine correctness in the first implementation stage

## 13. Testing Matrix

### 13.1 Direct Proxy

- TCP to IPv4 target with arbitrary port
- TCP to domain target with arbitrary port
- TCP to local loopback target
- UDP to remote target
- repeated short connections
- parallel connections over multiple Yamux substreams

### 13.2 TUN Gateway

- TCP local ports `80`, `443`, `5228`, `7897`
- repeated short-lived connections
- multiple concurrent connections using listener pool
- handle close and rearm behavior
- UDP datagram relay
- TUN enabled vs disabled startup path

### 13.3 Shared Tunnel

- fake header generation and verification
- `RelayRequest` serialization and parsing
- TCP request round-trip
- UDP request round-trip
- one substream failure does not poison others

## 14. Implementation Stages

### Stage 1: Shared Models and Protocol

Files to introduce or refactor:

- `src/shared/target.rs`
- `src/shared/relay_protocol.rs`
- `src/shared/errors.rs`
- tunnel setup helper module if needed

Teaching document to write after completion:

- `/docs/tech/01-shared-models-and-relay-protocol.md`

### Stage 2: Direct Proxy Refactor

Refactor `client.rs` to use shared models and shared handshake.

Teaching document to write after completion:

- `/docs/tech/02-direct-proxy-refactor.md`

### Stage 3: TUN Runtime Skeleton Refactor

Refactor `client_tun.rs` to introduce:

- `ListenerSpec`
- `ListenerPool`
- `SocketCtx`
- `SocketState`
- central polling helpers

Start with pool size `1` if needed, but architecture must be pool-ready.

Teaching document to write after completion:

- `/docs/tech/03-tun-runtime-skeleton.md`

### Stage 4: Listener Pool Activation

Enable configurable pool size greater than `1`.

Teaching document to write after completion:

- `/docs/tech/04-listener-pool-activation.md`

### Stage 5: UDP + TUN Alignment

Unify UDP relay behavior across direct proxy and TUN mode where practical.

Teaching document to write after completion:

- `/docs/tech/05-udp-tun-alignment.md`

### Stage 6: Hardening

- remove remaining lifecycle `unwrap()`
- improve tracing
- add targeted tests
- tune buffers

Teaching document to write after completion:

- `/docs/tech/06-hardening-and-observability.md`

## 15. Documentation Workflow Requirement

After each implementation stage:

- summarize what changed
- explain why the architecture changed
- explain the key data flow
- explain the main trade-offs
- store the teaching document under `/docs/tech/`

These documents are not optional.
They are part of the delivery standard and serve as implementation references for later stages.

## 16. Out of Scope for the First Refactor

- automatic TLS/Yamux reconnect strategy
- HTTP/3 or QUIC migration
- advanced zero-copy protocol framing
- DPDK, AF_XDP, or tokio-uring integration
- full metrics backend integration

These may be considered after the lifecycle architecture is stable.

## 17. Recommended Next Step

Create the implementation plan for Stage 1 first.

Do not begin pool-size expansion before:

- shared handshake is unified
- target modeling is centralized
- `client_tun.rs` no longer hardcodes a single global socket handle

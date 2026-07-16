# Knife15 Endpoint Socket-Rebind Recovery Architecture Spec

Date: 2026-07-16

Status: **LOCALLY IMPLEMENTED AND GATED; SHENZHEN M0 ACCEPTANCE PENDING;
NO FROZEN THROUGHPUT PARAMETER CHANGE**

Source baseline: `8e9daf98ac41a6c17dacaeee7255fcaec114d1ac`.

## Decision

Add one endpoint-owned liveness monitor to `TuicUpstream`. When an active TCP
or UDP workload causes aggregate QUIC UDP transmit progress but none of the
live connections on the shared Endpoint records receive progress for a
bounded interval, switch that same Quinn Endpoint to a newly bound UDP socket.

The operation uses Quinn's live `Endpoint::rebind_abstract` path. It preserves:

- every active QUIC `Connection` and connection ID;
- every active TUIC TCP stream and its local relay ownership;
- the endpoint-owned pre-accounting pacing service and its conservation state;
- congestion, flow-control, MTU, pool, D16, and application state.

Only the local UDP socket/source-port identity changes. Existing connection-
local reconnect remains the later fallback after QUIC declares a connection
closed.

This is an endpoint recovery mechanism, not a throughput tuning mechanism.

## Evidence Selecting The Seam

The exact Shenzhen bundle
`/tmp/mini_vpn_knife15_macos_20260716_110535.tar.gz` passed fresh baseline,
300-second direct continuity, smoke, and a full M0 cycle. During cycle 2,
conn0 and conn1 on one Endpoint timed out together after the endpoint had
served traffic for about 18 minutes. Reconnect kept using the same UDP socket
and timed out. TUN, endpoint pacing, resources, gateway/Exit ICMP, sing-box,
and iperf remained healthy.

Quinn already exposes the necessary deep seam. `Endpoint::rebind_abstract`
atomically replaces the Endpoint socket, sends `ConnectionEvent::Rebind` to
every active connection, recreates their UDP pollers, calls
`local_address_changed`, and wakes the Endpoint driver. Its vendored
`rebind_recv` test demonstrates continuity of an established connection.

The current mini_vpn seam cannot use that safely because socket bind, buffer
configuration, Tokio wrapping, and optional UDP send-service wrapping are
embedded only in initial endpoint construction. Preparatory refactoring will
extract that infrastructure policy before adding recovery behavior.

## Goals

1. Recover a shared Endpoint/UDP identity outage before the existing 15-second
   QUIC idle timeout destroys active TCP streams.
2. Require positive workload and transport evidence: active TCP/UDP work plus
   aggregate UDP TX progress followed by endpoint-wide RX stagnation.
3. Rebind at most once in one continuous no-RX episode.
4. Rearm only after a known connection packet arrives on the current, newly
   rebound socket, or after all workloads become inactive and a later workload
   starts a new episode. Traffic on Quinn's temporarily retained old socket
   cannot prove recovery.
5. Preserve the same Endpoint, connections, endpoint pacing service, and all
   frozen data-plane parameters.
6. Preserve both the Quinn-default UDP socket and the optional bounded
   send-adapter construction path.
7. Add exact, non-secret observability for trigger, socket generation,
   recovery, and failure.
8. Keep the monitor bounded and off the relay hot path.

## Non-Goals And Frozen Values

- Do not change D16 ownership, TUN feedback, close ordering, or self-wake.
- Do not change endpoint pacing rate, burst, control reserve, DRR quantum, or
  its conservation equation.
- Do not change MTU/PLPMTUD, pool size, QUIC stream/connection/send windows,
  application chunk, Cubic, GSO default, idle timeout, keepalive, or workload
  rates.
- Do not revive PacerCap64, the rejected bounded sender as a production
  candidate, GSO-only, or any constant-tuning branch.
- Do not relax receiver continuity or speed acceptance.
- Do not replace normal connection-local reconnect, failover, or TUIC auth.
- Do not claim a deterministic local rebind test proves Shenzhen WAN recovery.

## Safety And Liveness Contract

Let a monitor sample every `250ms`. For every live connection identity it
records monotonic QUIC UDP TX/RX byte counters and smoothed RTT. A sample is
usable only when every pool slot can be observed without awaiting its mutex.

An episode may arm only when:

```text
active_workload = active_tcp_leases > 0 OR recent_udp_uplink
tx_since_last_rx > 0
aggregate_rx_progress_since_arm = 0
```

Before rebind, any RX progress on any connection proves the shared Endpoint is
receiving and clears the episode. After rebind, only a connection packet
routed through the current socket advances the successful-recovery generation.
Quinn may temporarily retain the previous socket during active migration, so
its RX counters alone are insufficient positive recovery evidence. A
connection-local failure while another connection still receives does not
authorize Endpoint rebind.

The no-RX bound is derived from transport state:

```text
stall_bound = clamp(8 * max_smoothed_rtt, 2s, 7s)
```

The `2s` floor avoids reacting to one scheduler delay, isolated loss, or a
small number of QUIC recovery rounds. Eight RTTs gives a `164ms` Shenzhen path
more than twelve RTTs because the floor dominates. The `7s` ceiling plus one
`250ms` observation period keeps the action strictly before the frozen
15-second QUIC idle timeout, leaving at least `7.75s` for path validation and
RX recovery. These are liveness safety bounds, not bandwidth parameters.

After the first decision, `rebound_without_rx=true` suppresses every further
rebind in that episode. A bind/wrap/rebind error still consumes the one
attempt; local resource errors must not become a socket-creation loop.

Safety properties:

```text
rebinds_per_continuous_no_rx_episode <= 1
single_connection_stall + other_connection_rx => no rebind
inactive_workload => no rebind
tx_progress == 0 => no rebind
```

Liveness property:

```text
active endpoint-wide tx-without-rx outage
  => one rebind no later than stall_bound + 250ms
  => before QUIC idle timeout
```

## Capacity And Resource Math

The recovery monitor performs at most four samples per second over the frozen
two-slot pool: at most eight non-blocking connection-stat reads per second.
It adds no relay queue, payload copy, packet, pacing reservation, or await on
the event-loop hot path.

The endpoint pacing capacity is unchanged:

```text
rate             = 30,720,000 wire B/s
burst            = 61,440B
1ms envelope     = 92,160B
10ms envelope    = 368,640B
application cap  ~= 239.167 Mbit/s
```

The invariant remains:

```text
available_tokens + live_reservation_bytes + outstanding_bytes <= burst_bytes
```

Rebind creates at most one new socket per episode. Quinn retains only the
immediately previous socket while switching; a subsequent accepted episode
replaces that reference. Optional bounded-send adapter accounting is shared
across socket generations, so rebind cannot reset its service gate.

This change is sufficient at code level for changing socket identity without
losing established connections. It remains necessary-only for real Shenzhen
recovery and does not itself guarantee an M0 PASS.

## Components And Boundaries

### `quic.rs`: Socket Infrastructure Adapter

Apply Extract Function to separate:

1. wildcard UDP bind;
2. configured receive/send socket buffers;
3. runtime wrapping;
4. optional bounded send-adapter wrapping.

Initial endpoint construction and rebind call the same adapter. A rebind
result returns old/new local addresses and preserves the existing optional
send-service stats state.

### `tuic.rs`: Pure Recovery Policy

`EndpointRecoveryState` consumes only:

- `now`;
- active-workload state;
- per-connection stable ID, TX bytes, RX bytes, and RTT.

It returns `None`, `Rebind`, or `Recovered`. It performs no socket I/O. This
keeps the liveness policy deterministic and independently testable.

### `tuic.rs`: Endpoint Monitor Adapter

One task, started at most once with the UDP driver, samples pool connections
via `try_lock`, computes activity, applies the pure policy, and invokes the
socket adapter on `Rebind`. It uses a weak owner reference so it does not add
an independent `TuicUpstream` lifetime.

## Observability

One trigger line records:

- socket generation;
- old/new local addresses;
- active TCP count and UDP-active flag;
- live connection count;
- no-RX duration and computed bound;
- TX bytes since last RX;
- maximum RTT.

One recovery line records the policy generation, current-socket generation,
and first-current-socket-RX latency after rebind. A bind/wrap/rebind failure
records its stage and generation without secrets.
Connection IDs, UUIDs, passwords, keys, and target payloads are never logged.

The next macOS bundle must expose enough lines to distinguish:

1. no trigger because RX never actually became endpoint-wide stagnant;
2. correct trigger and post-rebind RX recovery;
3. correct trigger but no recovery;
4. a repeated-trigger invariant violation;
5. a local bind/wrap/rebind failure.

## Old-Path Audit

- `reconnect_locked` remains and still authenticates a new QUIC connection
  after a connection is closed. It will use the most recently rebound socket.
- UDP-driver reconnect/backoff remains.
- Failover health checks and five-second reconnect/open bounds remain.
- EndpointWindowV1 remains endpoint-owned and is not recreated by rebind.
- D16, smoltcp/TUN, TCP pool placement, and close lifecycle are unchanged.
- Quinn-default behavior before the recovery predicate is byte-for-byte
  unchanged except for four bounded read-only samples per second.

## TDD And Acceptance

1. Pure fake-time RED: active aggregate TX with no RX crosses the bound and
   produces exactly one `Rebind`; repeated samples do not.
2. Pure safety cases: any RX rearms, other-connection RX suppresses a
   connection-local stall, no workload/no TX suppresses action, and RTT bounds
   are exact.
3. Socket adapter test: initial construction and rebind apply the same buffer/
   send-service policy.
4. Local two-connection integration RED: one endpoint uses EndpointWindowV1,
   changes local UDP port, both established connections exchange data after
   rebind, and the same pacing snapshot remains valid.
5. Vendored Quinn tests prove old-socket traffic does not advance the current-
   socket RX generation and a path probe/response on the new socket does.
6. Monitor wiring tests lock once-only startup and non-blocking incomplete
   samples.
7. Run focused tests, vendored Quinn-proto gates, all-target Rust gates,
   release/Clippy, Knife15/Knife14 shell suites, fmt, and diff checks.
8. Code review must find no unresolved P0/P1 before macOS replay.

Local tests prove mechanism and invariants, not the WAN. The first repaired
Shenzhen M0 is the acceptance discriminator.

## Stop Rules

- An expected focused RED may enter its corresponding minimum implementation.
- An unexpected local repair/regression failure requires causal analysis; do
  not tune constants or broaden the change.
- If local two-connection rebind cannot preserve both live connections and
  endpoint pacing state, reject this architecture.
- If repaired Shenzhen M0 observes any receiver-zero interval before recovery,
  no rebind during an evidenced endpoint-wide outage, no RX after a rebind, or
  more than one rebind in an episode, reject the architecture and re-evaluate
  the boundary. Do not tune the liveness bounds from that run.

## Design Score

The pre-implementation recovery structure was **6/10**: connection-local
reconnect was tested and bounded, but it could not change a failed shared
socket identity; initial socket construction was not reusable; and endpoint-
wide liveness had no owner.

The locally implemented structure is **10/10** for the defined boundary:
one reusable socket adapter, one pure one-shot policy, one bounded endpoint
owner, deterministic multi-connection/current-socket tests, exact
observability, and no duplicate recovery authority. This is a structural
score, not Shenzhen WAN acceptance.

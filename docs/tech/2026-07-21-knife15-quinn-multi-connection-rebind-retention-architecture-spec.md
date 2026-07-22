# Knife15 Quinn Multi-Connection Rebind Retention Architecture Spec

Date: 2026-07-21

Status: **LOCAL IMPLEMENTATION AND REVIEW PASS; REAL M1 FAILED**

Source baseline: `1c587ba8bcc4c78c7fb77de72c1d40aba87f88f5`.

Failure evidence:
`/tmp/mini_vpn_knife15_macos_20260721_110225.tar.gz`, SHA-256
`62280ba0b638f73fee8647e2461f3a0880a08b6dd252f3e29624d09e5ae27715`.

## Decision

Deepen Quinn's live-rebind module so a shared Endpoint retains its previous UDP
socket until every connection that was live at rebind has either:

1. authenticated a connection packet received on the current socket
   generation; or
2. drained from the Endpoint.

The current implementation drops the previous socket when the first live
connection receives on the current socket. That is correct for one connection
but shallow for a pooled multi-connection Endpoint. The replacement keeps the
same public `Endpoint::rebind_abstract` operation while moving all generation,
pending-connection, old-socket-retention, and per-connection recovery knowledge
behind that Quinn seam.

mini_vpn's endpoint-recovery policy will consume per-connection current-socket
generations and declare `Recovered` only after all connections observed at the
successful rebind have recovered. Endpoint-level first-RX remains available as
a coarse compatibility statistic but is no longer sufficient recovery proof.

This is a lifecycle correctness repair, not a liveness-threshold or throughput
parameter change.

## Evidence Selecting The Seam

The exact-source HK M1 run was correctly operated and passed fresh physical
baseline and the 300-second direct discriminator. It completed four active
windows, all three idle/resume checkpoints, and entered `steady-c` before a
real failure in cycle 28 `short-reverse-6`.

The authoritative local receiver recorded eight complete approximately
one-second zero-byte intervals. The corresponding conn1 stream had an
`8,301ms` read gap; conn0 simultaneously recorded a `10,733ms` control-stream
read gap. At two seconds of Endpoint-wide TX without RX, mini_vpn rebound the
shared UDP socket. Quinn reported current-socket recovery after `1,499ms`, but
that proof could have come from either connection while conn1 remained without
usable stream data.

The local path stayed healthy: Target route on the owned utun, Exit and gateway
ICMP at zero loss, physical and utun interface errors zero, pump high-water
`113/500`, no pump full waits/read errors, no send-slice or TUN flush failures,
and exact EndpointWindowV1 conservation. The failure therefore selects the
connection/QUIC service architecture, not the runner, operator, TUN drain, D16,
or resource lifecycle.

Vendored Quinn contains the matching defect in
`third_party/quinn-0.11.11/src/endpoint.rs`: after any current-socket connection
packet it clears `prev_socket`, with the explicit note
`TODO: Account for multiple outgoing connections.`

The same M1 also exceeded the independent UDP SLO in two of 24 completed UDP
windows (`4.421980%` and `4.127822%`, maximum allowed `3.0%`). mini_vpn's local
UDP drop/backpressure counters and same-window interface errors stayed zero.
This repair does not claim to solve that external/TUIC-datagram quality result.

## Goals

1. Preserve the old receive path independently for every connection live at
   rebind instead of coupling its lifetime to the first recovered connection.
2. Expose the latest current-socket rebind generation received by each Quinn
   `Connection`.
3. Require all rebind-time live connections to prove current-socket recovery
   before mini_vpn emits the single recovered event.
4. Remove drained connections from the old-path pending set so a closed
   connection cannot retain a socket indefinitely.
5. Exclude connections created after rebind from the old-path pending set.
6. Preserve EndpointWindowV1, live connections and streams, socket send-adapter
   accounting, and all existing pool/TUN/D16 lifecycle behavior.
7. Keep packet processing bounded and allocation-free per received datagram.
8. Add exact tests and observability at the Quinn and mini_vpn interfaces.

## Non-Goals And Frozen Values

- Do not change the `2s..7s` endpoint recovery bound or `250ms` monitor cadence.
- Do not change D16, MTU `1200`, UDP payload `1160B`, pool `2`, QUIC windows,
  chunk size, Cubic, GSO default, self-wake, idle timeout, keepalive, M1 rates,
  M1 duration, or any receiver/UDP acceptance SLO.
- Do not re-open bounded sender, PacerCap64, GSO-only, or parameter tuning.
- Do not reconnect or replace a live QUIC connection merely because another
  connection has migrated.
- Do not duplicate a TUIC TCP stream across connections; standard TUIC has no
  byte-stream resumption/deduplication contract for that operation.
- Do not claim a deterministic loopback migration proves the WAN or the
  independent M1 UDP loss gate.

## Safety And Liveness Contract

At successful socket rebind generation `g`, snapshot the Endpoint connection
handles that are live at that instant:

```text
pending[g] = live_connections_at_rebind[g]
```

Only a packet received on the current socket and authenticated by connection
`c`, or a drain event for `c`, may remove it:

```text
authenticated_current_socket_packet(c, g) => pending[g] -= c
connection_drained(c)                   => pending[g] -= c
```

Packets on the retained previous socket do not change `pending[g]` or the
per-connection current-socket generation. Connections inserted after the
snapshot are absent from `pending[g]`.

The old socket lifetime invariant is:

```text
previous_socket.is_some() == !pending[g].is_empty()
```

except that an old-socket I/O error may release it early because it is no
longer usable. A subsequent explicit rebind replaces the one retained previous
socket and creates a fresh snapshot; Quinn never retains more than one previous
socket generation.

mini_vpn snapshots the stable IDs present when its rebind succeeds. It emits
`tuic-endpoint-rebind-recovered` only when every still-observed snapshot member
has `current_socket_rx_rebind_generation >= g`. A disappeared unrecovered
stable ID fails closed: it cannot be counted as a successful migration.

Safety properties:

```text
first recovered connection + another pending connection => retain old socket
old-socket packet                              => no recovery-generation advance
drained pending connection                     => cannot strand old socket
post-rebind new connection                     => cannot extend old socket lifetime
EndpointWindowV1 conservation                  => unchanged
```

Liveness property under the frozen 15-second connection idle timeout:

```text
each rebind-time connection either receives on generation g or drains
  => pending[g] eventually empty
  => previous socket eventually released
```

## Module And Seam Design

### Quinn Endpoint live-rebind module

The `Endpoint` interface remains one rebind call. Its implementation owns a
small pending-handle set next to `prev_socket`. `poll_socket` receives a socket
origin/generation marker; the connection driver returns proof only after QUIC
packet protection authenticates the packet, so authenticated current-socket
packets can:

- remove exactly their routed connection handle from the pending set;
- carry the current socket generation to that connection's driver; and
- advance the coarse Endpoint statistic without releasing another
  connection's old receive path.

This is the deep module: callers do not manage handle snapshots, socket-origin
classification, or drain races. Deleting it would spread those facts through
every rebind caller, so the module passes the deletion test and improves
locality and leverage.

### Quinn Connection recovery interface

Each connection stores one monotonic current-socket RX generation updated by
its driver when it handles a packet tagged by the Endpoint. A read-only method
exposes it. The interface is deliberately narrower than exposing Endpoint
handles or internal path objects.

### mini_vpn endpoint-recovery policy

`EndpointRecoveryConnectionSample` gains the per-connection generation. The
pure policy snapshots stable IDs on successful rebind and completes only when
all snapshot members prove generation `g`. Socket I/O remains outside the pure
policy.

## Capacity And Resource Review

The frozen Endpoint pacing capacity remains:

```text
rate             = 30,720,000 wire B/s
burst            = 61,440B
1ms envelope     = 92,160B
10ms envelope    = 368,640B
application cap  ~= 239.167 Mbit/s
```

and conservation remains:

```text
available_tokens + live_reservation_bytes + outstanding_bytes <= burst_bytes
```

The pending set has one entry per connection live at rebind. It is allocated
once per rebind, not per packet. Each connection reports at most once per
generation; the Endpoint then performs one bounded hash removal after Quinn has
authenticated the packet. The connection event adds one integer field and no
payload copy, pacing reservation, await, or new timer. Only one previous socket
is retained.

For mini_vpn's frozen pool of two, the additional state is exactly two stable
IDs plus two generation integers in the recovery sample.

## Old-Path Audit And Sufficiency

- Quinn connection migration and path validation remain authoritative.
- Quinn EndpointWindowV1 remains Endpoint-owned and unchanged across rebind.
- The old socket still receives packets through the same `proto::Endpoint`
  routing path; only its release condition changes.
- mini_vpn's one-rebind-per-no-RX-episode rule remains.
- Connection-local reconnect, pool selection, TUN/smoltcp, D16, fake-IP DNS,
  TUIC TCP Connect, and native UDP Packet paths remain active.
- The runner's strict receiver and UDP SLOs remain unchanged.

This repair is intended to be sufficient for the identified premature
old-socket-release defect. It is necessary-only for a future real M1 PASS: it
cannot guarantee that a WAN path accepts migration, and it cannot explain away
or waive the two UDP-loss violations already observed.

## Failure Discriminators

| Observation | Selected boundary | Action |
|---|---|---|
| first connection recovers and old socket disappears while another is pending | Quinn retention implementation | local regression; stop and repair |
| old-socket packet advances per-connection generation | Quinn origin tagging | local correctness failure |
| all current-socket generations advance but mini_vpn emits no recovery | pure policy/wiring | focused policy repair |
| one connection never advances and later receiver zeros repeat | connection migration/path | architecture still insufficient; do not tune constants |
| receiver continuity passes but UDP remains above 3% | TUIC datagram/external path quality | separate discriminator; do not alter TCP/rebind constants |
| Endpoint conservation or throughput gate regresses | pacing integration | reject the repair |

## TDD And Gates

1. Quinn RED: two rebind-time connection handles; recovering only the first
   must retain the previous socket and report only that connection's current
   generation.
2. Minimal GREEN: snapshot/pending ownership, socket-origin-tagged connection
   events, and authenticated per-connection proof.
3. Quinn follow-up cases: second recovery releases; drain releases; old-socket
   traffic and routed-but-unauthenticated traffic do not advance; a post-rebind
   connection does not join pending.
4. mini_vpn RED: endpoint-level generation plus only one recovered connection
   must not return `Recovered`.
5. Minimal GREEN: all-snapshot per-connection recovery proof.
6. Existing live two-connection rebind test must require both connection
   generations and preserve EndpointWindowV1 conservation.
7. Run focused tests, root Rust gates, vendored Quinn/Quinn-proto unit and doc
   gates, release/Clippy, Knife15/Knife14 shell suites, fmt, diff, and secret
   checks.
8. Code review must have no unresolved P0/P1 before another user-run macOS M1.

No real macOS TUN is run by the agent.

## Stop Rules

- Expected focused RED may enter only its corresponding minimal GREEN.
- Any unexpected local failure gets a causal analysis before broadening the
  change; frozen values and SLOs remain unchanged.
- A local test that cannot distinguish first-connection from all-connection
  recovery rejects the seam.
- A future real receiver-zero interval after this repair rejects sufficiency;
  it does not authorize threshold tuning.
- M2 and M3 remain blocked until a fresh complete M1 passes every gate,
  including UDP loss.

## Design Scores

- Current Quinn rebind structure: **6/10**. The public operation is small, but
  old-socket lifecycle and recovery evidence collapse multiple connections
  into one Endpoint boolean.
- Target structure: **10/10** for this seam. One deep rebind module owns all
  pending identities and socket-origin facts; the interface stays small and
  tests exercise observable per-connection recovery.
- System design: **10/10** for the bounded repair scope. Requirements,
  capacity, safety, liveness, observability, failure modes, old paths, and WAN
  limitations are explicit.

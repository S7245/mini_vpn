# Knife15 M2 Ambient-Traffic Recovery Architecture Spec

Date: 2026-08-05

Status: **LOCAL IMPLEMENTATION AND REVIEW ACCEPTED; REAL MACOS M2 PENDING**

## Stage Goal

Make the 24-hour full-tunnel acceptance valid on an ordinary logged-in macOS
host. Normal long-lived system TCP sessions may coexist with the controlled M2
workload without authorizing endpoint-wide socket migration or being
misclassified as a lifecycle leak.

This stage repairs two selected defects:

1. a silent established TCP relay currently makes aggregate QUIC ACK traffic
   eligible for the generic endpoint no-RX trigger, even when no business
   stream write is stalled;
2. the M2 runner requires every system relay, fake-IP lease, and pool lease to
   be zero, although full-tunnel macOS necessarily carries traffic that the
   harness does not own.

The repair must retain exact-stream ACK-stall recovery for TCP, endpoint-wide
no-RX recovery for recent UDP activity, and every existing controlled M2
quality, resource, conservation, route, DNS, cleanup, and evidence gate.

## Grounding Evidence

Exact-source `a1e22ca` bundle
`/tmp/mini_vpn_knife15_macos_20260805_080607.tar.gz`, SHA-256
`f5f6d93357f84d5b94cb8ae1a8eece19960023f4a6bb09e1fd0b8facee02ce7e`,
was operated correctly.

- baseline passed at `19.183/50.639 Mbit/s` with no receiver gaps;
- the 300-second direct discriminator passed at `9.584823 Mbit/s` with no
  sender or receiver gap;
- start, smoke, IPv6, full-tunnel, and real-client preflight passed;
- the new bounded pre-schedule gate stopped before the 24-hour schedule and
  cleanup passed;
- the final failed sample was Endpoint `61,414/0/0B`, DNS drops `0`, pool
  leases `2`, relay `1`, fake-IP active `1`, registered `9`.

The user's third-party Apps were closed. The sole relay at the decisive sample
was `28-courier.push.apple.com:5223`; preceding DNS and retries were Apple
Push/iCloud system traffic. The controlled `api.ipify.org:443` and
`example.com:443` preflight relays had already emitted their exact
`tcp-handle-close` lifecycle records.

The same silent Apple Push relay caused fifteen socket migrations in about
fifty seconds. Every trigger was:

```text
trigger=no_rx
active_tcp=2 udp_active=false
write_conn=0 write_writer=0 write_stream=0 write_episode=0
write_acknowledged=0B write_pending_ms=0
tx_since_rx=37B
```

Generations 1 through 14 recovered in about 250ms and immediately armed the
same condition again. This is a false-positive recovery loop caused by
transport ACK traffic plus ownership presence, not an application writer
stall, TUN failure, Endpoint pacing failure, path outage, or operator error.

## Design Tree

Rejected:

1. terminate `apsd`, sign out of iCloud, use Safe Mode, or require every
   background daemon to stop — this makes the acceptance host unlike the VPN
   product environment and delegates correctness to the operator;
2. allowlist Apple domains — target names are unbounded product input and a
   vendor list cannot define lifecycle ownership;
3. remove drain/checkpoint gates — this would hide controlled relay leaks,
   D16 ownership, Endpoint debt, and evidence corruption;
4. increase the two-second floor, monitor interval, or QUIC idle timeout — the
   predicate is wrong and a larger constant only delays the same false action;
5. treat any QUIC UDP TX as business demand — ACK/control packets do not prove
   that a TCP application writer is blocked.

Selected:

- deepen the pure Endpoint recovery policy so generic no-RX is demand-qualified
  by recent UDP application activity; TCP recovery remains owned by the exact
  pending writer and its exact stream ACK-stall episode;
- deepen the runner's lifecycle replay module so controlled M2 handles are
  reconstructed from immutable open/install/closing/close events at each
  data-plane sample;
  controlled ownership must drain to zero while ambient system ownership is
  preserved as bounded observation.

## Recovery Contract

The existing `TcpWriteStall` predicate is unchanged:

```text
pending_age(writer) >= stall_bound
AND ack_stall_age(exact_stream) >= stall_bound
AND writer_episode_not_already_covered
```

The generic endpoint no-RX predicate becomes:

```text
recent_udp_application_activity
AND aggregate_quic_tx_since_rx > 0
AND aggregate_endpoint_rx_progress == 0
AND elapsed >= clamp(8 * max_rtt, 2s, 7s)
```

`active_tcp_leases > 0` alone cannot arm or retain generic no-RX state. For a
TCP-only sample without an eligible exact writer stall, the generic no-RX
episode is cleared. This prevents prior UDP demand from leaking into a later
TCP-only epoch.

Safety:

```text
silent_tcp + no_pending_writer => no endpoint rebind
tcp_pending + exact_stream_ack_progress => no endpoint rebind
tcp_pending + exact_stream_ack_stall => one endpoint rebind
recent_udp_tx + endpoint_wide_no_rx => one endpoint rebind
```

Authenticated current-socket recovery, one action per episode, connection
replacement, bind failure handling, and later connection-local reconnect are
unchanged.

## Controlled-Lifecycle Replay Contract

The runner owns three classes of M2 TCP Target:

```text
${target}:${iperf_port}
api.ipify.org:443
example.com:443
```

These are workload identities, not a domain allowlist. An adapter replays the
existing log stream:

- `tuic-open-tcp ... target=T ... handle=SocketHandle(H) epoch=E` stages the
  remote-open result but does not yet prove local installation;
- matching `tcp-relay-engine handle=SocketHandle(H) epoch=E` installs the
  relay into the current local socket epoch; a stale successful async open
  never reaches this boundary and its existing exact stale-result record
  retires the staged entry;
- the first `tcp-lifecycle-transition ... ctx_state=Closing` or
  `tcp-handle-close` ends active relay ownership. Final handle close may occur
  much later while smoltcp completes FIN/TIME_WAIT and is not transport
  ownership;
- every `📊 数据面` row materializes replayed Relaying handles and controlled
  active handles;
- malformed/duplicate lifecycle input fails closed.

A staged controlled open is conservatively counted as controlled ownership
until it installs, closes, or is proven stale. This prevents a checkpoint
sample from racing an async `HandshakeDone` installation.

Do not equate replayed Relaying handles with the published global gauge. Real
four-hour evidence produced `516` differences because the gauge samples
`SocketCtx` on a periodic boundary while final lifecycle records have a
different boundary. At the actual idle-1 sample, the corrected replay yields
four ambient Relaying handles and zero controlled handles, exactly matching
the selected ownership question without making global equality an invariant.

At pre-schedule drain and every six checkpoint:

```text
controlled_active_relays == 0
replay_invalid == 0
endpoint_live_bytes == 0
endpoint_outstanding_bytes == 0
endpoint_available + live + outstanding <= 61,440B
dns_dropped == 0
```

Global pool leases, relays, fake-IP active entries, and registered fake-IP
entries remain numeric observations. They are not required to be zero or a
fixed count because the runner does not own normal system/App traffic. RSS,
FD, thread, log, D16 terminal ownership, Target result, TCP gap, UDP loss,
route/DNS, real-client, and cleanup gates remain fail-closed.

Checkpoint capture may use the existing bounded smoke timeout after the
600-second drain to obtain a fresh qualifying Endpoint/data-plane sample. It
does not alter any scheduled traffic or drain duration.

## Capacity And Reachability

The Rust policy change is O(1) inside the existing 250ms monitor. It adds no
payload queue, copy, lock, packet, pacing reservation, writer, or timer.

Lifecycle replay is an offline/checkpoint adapter over the immutable log. It
runs before the schedule and at six checkpoints, never in the relay or packet
hot path. Its memory is bounded by simultaneously active TCP handles; its time
is linear in retained evidence and is covered by the existing log/disk bounds.

Endpoint pacing remains:

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

This repair is intended to be sufficient for the selected false-rebind and
ambient-ownership observer classes. It does not itself prove the 24-hour WAN
acceptance or claim a new throughput path.

## Old-Path Audit And Frozen Values

- TCP exact-stream ACK-stall recovery remains active.
- UDP endpoint-wide no-RX recovery remains active.
- authenticated set-wise current-socket recovery and connection reconnect
  remain active.
- M2 controlled traffic, receiver continuity, `16MiB` TCP-gap, `3%` UDP-loss,
  resource, route/DNS, real-client, cleanup, and immutable evidence gates
  remain active.
- D16, MTU1200, UDP1160, pool size 2, QUIC windows, chunk sizes, Cubic, GSO
  default, self-wake, pacing constants, recovery cadence/bounds, full-tunnel
  route scope, IPv6 non-goal, and all SLO values are frozen.
- PacerCap64, bounded sender, GSO-only, pool/MTU/window/chunk tuning, and domain
  allowlists remain closed.

## TDD And Failure Discriminators

1. RED: artifact-shaped TCP-only samples with active leases, no writer
   pressure, repeated 37-byte QUIC TX, and no RX must never rebind.
2. RED/GREEN: the same policy must still rebind recent UDP endpoint-wide
   no-RX and exact TCP stream ACK-stall.
3. RED: lifecycle fixtures with one ambient open relay and all controlled
   handles closed must pass controlled drain while retaining global counts.
4. RED: a controlled installed/Relaying handle, malformed duplicate
   lifecycle, live Endpoint debt, or DNS drop must fail. Gauge/replay
   differences remain visible but are not ownership failures.
5. Checkpoint fixtures must accept changing ambient/fake-IP counts while
   rejecting any controlled ownership or replay inconsistency.
6. Run complete Rust, shell, vendored Quinn/proto, release, Clippy, formatting,
   diff, secret, and exact 32MiB capacity gates; review P0/P1 before macOS.

Real acceptance discriminator:

- no `trigger=no_rx ... udp_active=false` action may occur;
- any TCP rebind must identify a qualifying exact writer episode;
- ambient system relays may remain, but every controlled M2 relay must be
  closed at all six drains and final cleanup;
- all existing M2 SLIs and cleanup must pass.

## Stop Rules

- Expected focused RED may enter its minimum implementation.
- Unexpected repair/regression failures require causal analysis and scoped
  repair; no invariant or SLO may be weakened.
- Local 32MiB Endpoint capacity `<=170 Mbit/s` rejects the architecture; do
  not tune constants.
- Any malformed or missing controlled lifecycle evidence fails closed; a
  global gauge/replay difference is observation because their boundaries are
  intentionally different.
- A fresh real M2 failure with healthy controls but installed-successor
  receiver-zero evidence reopens Quinn initial-stream scheduling/failover
  research; it does not reopen parameter tuning.

## Design Score

The artifact-selected structure is **8/10** before implementation: recovery
and acceptance each have a testable policy seam, but both currently authorize
actions from ownership they do not semantically own. The target is **10/10**:
exact demand owns recovery, exact workload identity owns drain acceptance,
ambient state remains observable, and immutable replay checks its own
consistency.

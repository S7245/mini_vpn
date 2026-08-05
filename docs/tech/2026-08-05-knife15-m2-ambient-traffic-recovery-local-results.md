# Knife15 M2 Ambient-Traffic Recovery Local Results

Date: 2026-08-05

Status: **LOCAL ACCEPTED; FRESH REAL MACOS M2 PENDING**

## Outcome

The exact-source `a1e22ca` artifact
`/tmp/mini_vpn_knife15_macos_20260805_080607.tar.gz` (SHA-256
`f5f6d93357f84d5b94cb8ae1a8eece19960023f4a6bb09e1fd0b8facee02ce7e`)
did not fail because the operator left a user App open. It passed baseline,
direct continuity, start/smoke, IPv6/full-tunnel/real-client preflight, the
bounded pre-schedule stop, and cleanup. The remaining relay was
`28-courier.push.apple.com:5223`, normal Apple Push/iCloud system traffic.

The artifact selected two semantic defects:

1. silent TCP ownership plus 37-byte QUIC ACK/control progress repeatedly
   armed generic endpoint no-RX despite `udp_active=false` and no exact TCP
   writer pressure;
2. M2 treated all macOS traffic as harness-owned and required global
   relay/lease/fake-IP zero, which is not a valid full-tunnel product model.

Both defects are repaired without changing any frozen data-plane value or SLI.
The reviewed implementation is commit `1debaff`.

## Artifact Discriminators

- Baseline passed at `19.183/50.639 Mbit/s`, with zero receiver gaps.
- The 300-second direct discriminator passed at `9.584823 Mbit/s`, with zero
  sender and receiver gaps.
- The failed pre-schedule sample remained healthy at Endpoint
  `61,414/0/0B`, DNS drops `0`, leases `2`, relay `1`, and fake-IP `1/9`.
- Controlled `api.ipify.org:443` and `example.com:443` handles had closed.
- The Apple Push relay caused fifteen `trigger=no_rx` migrations in about
  fifty seconds. Every event reported `udp_active=false`, zero writer/stream/
  episode fields, zero acknowledged/pending fields, and `tx_since_rx=37B`.

This rejects operator error, path outage, TUN, D16, pacing, Endpoint debt,
MTU, window, pool, and parameter-tuning branches.

## Implemented Recovery Contract

`EndpointRecoveryState` retains exact TCP writer recovery unchanged. Generic
endpoint no-RX now requires an advance of the existing UDP application
activity timestamp while UDP remains recent. A TCP-only sample without an
eligible exact writer clears the generic episode, so a silent TCP relay plus
transport ACKs cannot migrate the shared socket.

The change is O(1) in the existing 250ms monitor. It adds no packet-path
atomic, queue, copy, writer, timer, threshold, or pacing reservation.

Focused artifact-shaped coverage proves:

- silent TCP transport TX cannot arm or retain generic no-RX;
- exact TCP writer/stream ACK stall still produces one recovery action;
- recent UDP endpoint-wide TX/no-RX still produces one recovery action;
- current-socket set-wise recovery and connection replacement remain intact.

## Implemented M2 Ownership Contract

A pure BSD-awk adapter reconstructs exact TCP lifecycle from existing
`tuic-open-tcp`, matching epoch `tcp-relay-engine`, stale-open,
`ctx_state=Closing`, and `tcp-handle-close` records. The three controlled
workload targets are the profile's iperf Target plus the frozen api.ipify and
example.com probes. They are workload identities, not a product allowlist.

Pre-schedule drain and all six checkpoints now require:

```text
controlled_active_relays == 0
replay_invalid == 0
endpoint_live_bytes == 0
endpoint_outstanding_bytes == 0
available + live + outstanding <= 61,440B
dns_dropped == 0
```

Global pool leases, relay counts, and fake-IP counts remain numeric evidence.
They no longer reject normal system traffic. A staged controlled open remains
owned until exact install/close/stale retirement, preventing an async-open
race. `ctx_state=Closing` ends transport ownership even when the smoltcp final
handle close is delayed.

The runner does not equate replayed Relaying handles with the periodic global
gauge. The exact four-hour artifact produced `516` boundary differences. The
corrected replay at its idle-1 checkpoint was four ambient Relaying handles
and zero controlled handles. The latest artifact's decisive prefix was one
ambient handle and zero controlled handles.

## Verification

All relevant local gates pass:

- focused Endpoint recovery: `9/9`;
- root all-targets: library `672` passed plus `3` ignored, binary `2/2`;
- harness integration: `10` passed plus `4` ignored;
- release build: PASS;
- Knife15 internal and wrapper self-tests, Bash syntax: PASS;
- Knife14 low-RTT, suite, and sing-box-control self-tests: PASS;
- vendored Quinn: `37` passed plus `3` ignored; doc `1/1`;
- vendored quinn-proto: `309/309`; doc `3/3`;
- `cargo fmt --all -- --check` and `git diff --check`: PASS;
- established Clippy non-dependency lane: PASS with existing warnings and no
  new warning in the changed code;
- exact 32MiB Endpoint loopback gate: `235.232 Mbit/s`, final conservation
  `61,440/0/0B`, zero socket would-block.

An exploratory `-D warnings` invocation on Rust 1.95 surfaced nineteen
pre-existing style/API lints outside the changed regions. It is recorded as
toolchain/baseline debt, not hidden and not expanded into this lifecycle fix.
The first standalone vendored Quinn command also bypassed the root patch and
selected registry quinn-proto; rerunning with the explicit local patch passed.

## Review

Recovery authority, UDP reachability, exact-writer episode coverage,
lifecycle ordering, pending-open race, handle reuse, malformed evidence,
global-gauge boundary differences, checkpoint timing, bounded log cost,
macOS Bash/awk portability, old paths, SLO drift, and cleanup were reviewed.
No unresolved P0/P1 remains.

Frozen D16, MTU, UDP payload, pool, QUIC windows, chunk, Cubic, GSO default,
self-wake, Endpoint pacing, recovery bounds, route scope, workload, and SLOs
are unchanged.

## Next Discriminator

Pull the pushed reviewed descendant, rebuild release, keep Clash-TUN and every
other VPN/proxy off, and take one fresh uninterrupted real-Mac transaction:

```text
m2-ipv6-check -> baseline -> direct-discriminator -> start -> smoke -> m2 ->
status -> stop
```

Normal Apple Push/iCloud traffic may remain. Avoid deliberate heavy non-test
traffic. Any failure after `start` still requires `status/snapshot/stop`.
M3 remains blocked until the complete M2 schedule and cleanup pass.

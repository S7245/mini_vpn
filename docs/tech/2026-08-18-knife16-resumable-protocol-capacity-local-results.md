# Knife16 Resumable Protocol and Capacity Local Results

Date: 2026-08-18

Status: **TASK 2 PASS; TASK 3 NEXT; M3 BLOCKED**

## Outcome

Knife16 Task 2 is complete as a pure, transport-independent fixed core. The
new `resumable` module defines the v1 frame/record codec, authenticated attach
authority, checked replay-capacity model, directional TCP ownership windows,
and a session reducer. It performs no socket, Tokio, QUIC, TUN, or Target I/O.

This is a necessary local architecture stage, not a throughput or WAN claim.
It does not authorize a macOS/VPS run. Task 3 must place a deep
`OwnedUpstream` adapter behind the existing client call sites while preserving
the current TUIC behavior.

No D16, Endpoint, MTU, pool, QUIC window, chunk, Cubic, GSO, self-wake,
Knife15 workload, or quality threshold changed.

## Fixed protocol surface

The v1 envelope has a fixed 12-byte header and an independently versioned
session protocol. The implemented record set is:

- `ATTACH`, `ATTACH_ACCEPTED`, and authenticated
  `ATTACH_GENERATION_STATUS`;
- `OPEN` and stable `OPEN_RESULT`;
- directional `DATA(flow, offset, bytes)`;
- directional `ACK(next_accepted, final_accepted)`;
- directional `CLOSE(final_offset)` and `RESET(reason)`.

`SessionFlowId` is a nonzero session-scoped `u64`, deliberately distinct from
the legacy TUIC UDP `flow-id`/`assoc-id` `u32`. Frame version and negotiated
session version are separate types/constants.

All record kinds have literal golden byte vectors. The codec rejects bad
magic/version/type/flags, invalid fixed and variable lengths, truncation,
trailing bytes, zero identities, invalid directions/results/reasons/booleans,
oversized DATA, offset overflow, invalid targets, and negotiation drift before
unbounded buffering or allocation.

OPEN supports arbitrary nonzero-port IPv4/IPv6 and exact nonempty, NUL-free
UTF-8 resolver input up to 253 bytes. The protocol does not apply IDNA or DNS
canonicalization. Scoped IPv6 is rejected at this cross-host boundary.

Public construction normalizes DATA and domain ownership. A small slice or
one-byte string cannot retain an uncharged large caller backing allocation;
the 64 MiB-domain and oversized-`Bytes` regressions are GREEN. `Debug` output
redacts payload, Target, session, nonce, proof, exporter, and credential
material.

## Authentication and generation ownership

The attach transcript binds frame version, owner identity, ALPN, TLS exporter,
device principal, session id, exact next generation, fresh nonce, session
version range, and feature offer/requirements. Independent device and resume
secrets are combined with nested HMAC-SHA256. An independent Ruby/OpenSSL test
vector fixes the 187-byte transcript and final proof
`48a657e42cdd46ed1334297189cb6982559a2e99c287cbfa746ef4d607bb7b3f`.

The server commits exactly `current + 1` with atomic compare-exchange. The
client validates exact session, nonce, generation, version, and negotiated
features in `ATTACH_ACCEPTED`. If that response is lost after server commit, a
proof-valid stale request receives a correlated generation-status record and
derives only the exact next request with a fresh nonce. Unknown session and
bad proof retain the same public rejection.

The pure response validator assumes the adapter preserves the exact TLS
frame-to-leg association. Task 3 must make that provenance a typed transport
boundary; it is not delegated to an unstructured caller convention.

## TCP and session ownership

For each direction, the sender retains exactly `[peer_acked, next_sent)` and
the receiver never advances `accepted` over a gap. In-window reorder and
identical overlap are bounded and deduplicated; conflicting overlap,
out-of-window input, ACK regression/beyond-sent, conflicting final offsets,
and DATA after close/abandon fail closed.

The session reducer uses stable session/flow/operation capabilities for local
OPEN completion, sink writes, sink half-close, and tombstone expiry. These
callbacks remain valid across an authenticated leg replacement; peer frames
still require the exact current `CommittedLeg` and generation before any flow
mutation.

Application ACK advances only after the adapter reports actual sink
acceptance. Decode, queue admission, and terminal abandonment never mint an
ACK. FIN is two-phase: `final_accepted=true` is sent only after local sink
half-close succeeds; failure emits RESET. DATA/CLOSE received before Target
OPEN completion stays buffered, and rejection releases it without offering a
nonexistent sink.

Terminal flows release replay/receive ownership immediately and leave a
count-bounded tombstone for idempotent terminal-control replay. Monotonic flow
high-water marks prevent delayed OPEN/DATA from resurrecting a retired ID, so
`max_flows` is a live-flow bound rather than a lifetime-open limit.

The aggregate receive-budget transaction has one algorithm owner:
`TcpReceiveWindow` creates a crate-private reservation, the session checks the
global budget, and the same window immediately commits or drops it. The older
duplicated coverage model was removed. External callers see only atomic
`receive`; the reservation is intentionally not a public cross-window
capability.

## Capacity and copy characterization

The exact checked formula is:

```text
bytes/direction = ceil(rate_bps * effective_horizon_ns / 8,000,000,000)
effective_horizon = blackout_budget + max_normal_application_ACK_age
```

For a 500 ms total retained horizon:

| Rate | Bytes/direction | Full duplex | 64 KiB records/direction |
|---:|---:|---:|---:|
| 100 Mbit/s | 6,250,000 | 12,500,000 | 96 |
| 170 Mbit/s | 10,625,000 | 21,250,000 | 163 |
| 240 Mbit/s | 15,000,000 | 30,000,000 | 229 |

Per-flow and aggregate directional rates are separate inputs; aggregate
capacity is never derived as `per_flow * flow_count`. If 500 ms means an
additional blackout after ordinary ACK lag, that lag must be added to the
horizon rather than hidden in a constant.

For one flow and the protocol maximum 64 KiB ownership allocation, checked
persistent-backing ceilings are `6,299,151B`, `10,674,151B`, and
`15,049,151B` at 100/170/240 Mbit/s. The corresponding worst geometric
retained-suffix compaction copies per full drain are `2,083,299B`,
`3,541,608B`, and `4,999,921B`. A small adversarial model proves that a 64-byte
allocation drained by 47 bytes and refilled can retain 111 backing bytes and
copy 21 bytes during geometric compaction; bounds use the maximum legal wire
allocation, not an intended coalescer width.

These are TCP ownership-layer bounds. Inbound DATA currently has one codec
compaction copy plus one ownership copy; replay/peek views are zero-copy;
outbound frame serialization performs another copy. Task 7 must measure the
real adapter hot path before selecting production geometry or claiming
`>170 Mbit/s`.

All rate, duration, product, sum, `u64`, `u128`, full-duplex, segment, backing,
and simulated 32-bit `usize` overflow paths fail closed.

## Review findings closed before integration

Concentrated review found and closed the following P1 classes before any WAN
test:

- ATTACH acceptance lacked exact request/session/nonce correlation;
- a lost acceptance could leave client/server generations ambiguous;
- async sink callbacks were incorrectly at risk of being tied to an old leg;
- FIN acknowledgement could precede successful sink half-close;
- terminal flow entries could consume `max_flows` forever;
- DATA could be offered before Target OPEN acceptance;
- logical segment geometry understated legal physical backing/copy cost;
- invalid or large-backing targets could enter state before normalization;
- duplicated receive preview/commit logic could diverge after mutation;
- a public receive reservation was not bound to a specific window.

Final protocol/auth/capacity and session/lifecycle reviews report no unresolved
P0/P1. Deferred work is explicit: typed TLS-leg provenance in Task 3, control
priority under replay in Task 4, hot-path copy measurement in Task 7, and
secret zeroization/fuzz/abuse closure in Task 8.

## Local gates

- resumable focused unit tests: `94/94` PASS;
- root library: `807/807` PASS, `3` ignored;
- all targets: root `807 + 3 ignored`, main `2/2`, protocol integration
  `19/19`, public session API `1/1`;
- harness library: `819/819` PASS, `3` ignored;
- concurrency harness: `10/10` PASS, `4` ignored;
- release build: PASS;
- Clippy with only the repository's four documented legacy allowances: PASS;
- rustdoc: PASS with existing unrelated warnings;
- vendored Quinn: `40/40` PASS, `3` ignored; doctest `1/1` PASS;
- vendored quinn-proto: `330/330` PASS; docs `3/3` PASS;
- `cargo fmt --check` and `git diff --check`: PASS.

The first sandboxed Quinn loopback invocation was invalid evidence because the
sandbox denied local UDP sockets; the identical offline/local-proto command
passed outside that permission boundary.

## Next

Proceed to Task 3 only: introduce the deep `OwnedUpstream` abstraction and
adapter contract tests while preserving `TuicUpstream` and
`FailoverUpstream`. Do not start a WAN test or claim the Knife16 continuity or
throughput target from this pure fixed core.

# Knife16 R6 Actor-Inbound Composition Local Results

Date: 2026-08-24

Status: **LOCAL PASS; NECESSARY-ONLY; PRODUCTION SESSION ROOT,
ACTUAL-CLOSE/SILENT-FAILURE, R7-R9, TUN, VPS, WAN, THROUGHPUT, TASK 4, AND
M2 NOT ACCEPTED**

## Outcome

One real Quinn raw inbound record can now cross the production-shared fixed
actor, `QuinnClientSessionComposition`, exact controller slot, `LegIo`, and
client reducer without borrowing actor-local policy state or exposing raw
ownership. Current B remains receive-capable while retired A is in its
split-phase join. Retired A accepts only its own late inbound while it is still
running. Physical DELIVERED and resumable application ACK remain distinct.

This closes the previously unreachable raw-inbound composition seam. It does
not yet convert actor terminal/fatal state into the controller's exact
`LegLost` transaction and cannot by itself solve the one-second M2 continuity
failure.

## Accepted implementation

- `ClientSessionController` records the authenticated initial attach's
  `FeatureSet`. Its Quinn-composed current/retired receive adapters use that
  stored authority; the outer owner cannot supply decode features.
- `EncodedDelivery::from_quinn_actor_ingress` is the sole gate-owned adapter
  from actor `Bytes` into the production-shared delivery model. It moves the
  existing backing with copy index zero. `EncodedDelivery` is non-Clone in a
  production build.
- `StartedQuinnLegActor::try_transact_controller_inbound` performs at most one
  non-blocking dequeue. One actor-private pending slot restores the exact raw
  ownership on a defensive policy block; it retains the original queued
  marker, Ib/Cm permits, backing, and optional DELIVERED reservation.
- `QuinnClientSessionComposition::try_receive_current` checks only the exact
  current runtime/policy incarnation, so B remains live while A is Joining.
  `try_receive_retired` separately checks only a still-running exact retired
  incarnation. Neither path exposes `LegIo`, FIFO, receiver, permits, or raw
  bytes.
- A controller disposition impossible for the selected current/retired path
  becomes the actor's first protocol fatal inside the synchronous callback,
  before physical DELIVERED can commit.
- Actor cancellation drops the private pending record before publishing
  cancellation. Join still proves zero message/byte ownership and no pending
  physical ACK.
- Readiness publication now uses a monotonic maximum. Concurrent producers may
  reserve epochs out of completion order without rolling the watch value back
  or losing a registered waiter.
- Large failure/lifecycle ownership is boxed only at cold control boundaries.
  On rejection the exact actor, `LegIo`, FIFO, accepted capability, and input
  are moved back out and restored; the raw inbound hot path gains no allocation.

The bridge adds no task, channel, queue, message permit, byte permit, payload
copy, timeout, or capacity increase. Its only new slot holds ownership already
charged to the actor's checked envelope.

## TDD and review repairs

The accepted positive tracer is embedded in the complete real A-to-B lifecycle
test
`exact_clean_join_of_retired_a_clears_only_a_while_active_b_keeps_exchanging`.
It proves that a real B `OpenResult` is blocked before dequeue/decode while the
controller is Recovering, then applies exactly once after recovery. The same
test also covers real B inbound while A is Joining, late retired-A discard,
ordered DELIVERED versus application ACK, wrong current/retired actor identity,
pending-record cancellation, final shutdown, and zero ownership.

The independent negative tracer
`real_quinn_composition_wrong_route_is_first_fatal_never_acknowledges_and_joins_zero`
forces a controller-owned wrong route after a real actor dequeue. It proves
one shared first fatal cause, no physical DELIVERED, no controller mutation,
and structural zero-ownership join.

Meaningful RED/review findings repaired during this slice:

1. readiness originally incremented an atomic and then independently replaced
   a watch value; two producers could publish epoch 2 then epoch 1 and strand a
   waiter;
2. impossible current/retired dispositions were initially mapped only after
   the actor had committed physical DELIVERED;
3. production `EncodedDelivery` retained a broad `Clone`, weakening the exact
   raw ownership proof;
4. wrong-identity coverage initially lacked a queued real raw record and the
   active-B-while-A-Joining receive case;
5. the first wrong-route assertion incorrectly expected physical inbound and
   rejection counters not to move, even though failure correctly occurs after
   dequeue and before controller binding/DELIVERED; and
6. strict Clippy exposed oversized lifecycle/error enums. Cold ownership was
   boxed without adding a lint allowance or changing rollback semantics.

## Local validation

- focused actor module: `49/49`;
- focused controller: `10/10`;
- focused `LegIo`: `28/28`;
- focused two-leg scheduler/model: `32/32`;
- typed provenance: `130/130`;
- exact real Quinn retired-A/current-B composition tracer: `100/100`;
- exact real Quinn composition-fatal/no-ACK tracer: `100/100`;
- all targets/all features: library `1159 passed; 3 ignored`, main `2/2`,
  concurrency `10/10; 4 ignored`, protocol `25/25`, session API `1/1`;
- four explicitly ignored concurrency sweeps: `4/4`;
- real-smoltcp adapter: `2/2`, with its frozen-binary core tracer `100/100`;
- development/release all-target/all-feature checks, release build, and strict
  production-library Clippy with the four documented baseline allowances:
  PASS;
- rustdoc: PASS with `16` existing warnings outside this slice;
- isolated vendored Quinn, with `cargo tree` proving the exact absolute local
  quinn-proto `0.11.16`: `47 passed; 3 ignored`, one integration test ignored,
  doctest `1/1`; vendored quinn-proto: `330/330`, doctests `3/3`;
- root rustfmt, `git diff --check`, generated-artifact check, and focused
  tracked/untracked secret-shape scan: PASS.

Three independent final-tree reviews of ACK/error linearization, lifecycle/
ownership, and architecture/range each report unresolved P0/P1/P2 `0/0/0`.
The architecture review rates the current seam `8.5/10`; the disclosed
production-root absence and narrow opaque-gate dependency account for the
remaining score and are outside this necessary-only slice.

No macOS TUN, VPS, WAN, throughput, M2, M3, user traffic, or external resource
was used.

## Remaining boundary and next step

The production TUN entry still selects `LegacyOwnedUpstream`; no production
session nursery/root continuously drives this composition. R6 still lacks the
exact current-actor terminal/fatal-to-controller transaction, silent-failure
deadline arbitration, checked aggregate recovery-capacity closure, and the
complete real-loopback actual-close/silent-failure matrix. All R7 reverse-only
no-oracle/hint/rejection/race gates, R8 fault matrix, R9 real-smoltcp
replacement parity, enhanced owner/Target, UDP two-leg behavior, WAN,
throughput, and M2 remain open.

The next bounded stage is the exact actor-terminal composition transaction:
bind the current actor's physical fatal/terminal witness to the same controller
slot, emit at most one exact `LegLost`, preserve unrelated/standby ownership,
and prove actual close plus silent failure under deterministic deadlines before
adding the production nursery/root. Do not run TUN, VPS, WAN, throughput, or
M2 from this result.

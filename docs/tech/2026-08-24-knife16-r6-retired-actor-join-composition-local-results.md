# Knife16 R6 Retired-Actor Join Composition Local Results

Date: 2026-08-24

Status: **EXACT RETIRED-A LIFECYCLE BLOCKER ACCEPTED LOCALLY AS A
NECESSARY-ONLY SLICE; R6 ACTUAL-CLOSE/SILENT-FAILURE/DEADLINE/CAPACITY
REMAINDER, ACTOR-INBOUND COMPOSITION, R7-R9, UDP, TUN, VPS, WAN, THROUGHPUT,
TASK 4, M2, AND M3 NOT ACCEPTED**

## Outcome

The concrete lifecycle blockage after a successful A-to-B replacement is now
closed in production-shared local code. One crate-private composition owner
binds the controller's exact physical-leg policy half to the fixed Quinn
actor's exact runtime half. It retains retired A until A's real root, four
children, connection, Endpoint-map entry, ConnectionDriver references, private
Endpoint/EndpointDriver/socket ownership, messages, and bytes have completed.
Only the exact actor-join witness may then synchronously clear only the exact
controller-retired A slot.

The clear path emits no `LegLost`, changes no supervisor/recovery state, and
does not disturb active B. Retired capacity remains occupied while A is still
joining and becomes reusable only after the joined witness and exact terminal
are consumed together. This removes the previous permanent
`RetiredCapacityExceeded` state after the first replacement.

This is a necessary lifecycle and bounded-capacity result, not an M2 result.
It does not yet prove that a real close or silent blackout selects B within one
second, that reverse-only failure can switch without a fault oracle, or that
the real TUN path can drive this composition.

Architecture contract:
`docs/tech/2026-08-24-knife16-r6-retired-actor-join-composition-architecture-spec.md`.

## Implemented transaction

### Exact physical incarnation

- The fixed actor factory mints one opaque physical identity as two
  non-cloneable, non-copyable halves. The runtime half stays with
  `StartedQuinnLegActor` and crosses only its real joined witness; the policy
  half moves inside the exact actor-created `LegIo` consumed by the client
  controller.
- Equality uses private `Arc::ptr_eq` authority. It is not derived from or
  replaceable by `LegId`, generation, route, address, certificate, or a raw
  integer. Test memory transports carry no physical actor identity.
- A successful registered replacement first commits the controller's A-to-B
  move and then infallibly mirrors that same move in the composition's actor
  slots, with no await or fallible operation between the two publications.

### Split-phase retired join

- `begin_retired_join` verifies both current and retired physical bindings
  before cancellation. A mismatch returns both actors and controller slots
  untouched.
- Only retired A moves into one bounded composition-owned Tokio cleanup task.
  Repeated starts return `AlreadyJoining`; no task, queue, or byte reservoir
  multiplies.
- `try_finish_retired_join` is non-blocking. `Pending` retains the exact task
  and controller capacity; a semantic actor error restores the exact actor for
  retry; structural cleanup-task loss remains sticky and fail-closed.
- A successful join persists the exact joined witness and synchronously calls
  the retired-only controller transaction. There is no await window between
  witness persistence and clear. Terminal/identity mismatch retains evidence
  and does not free capacity.
- Synchronous `Drop` is documented only as an unclean abort fallback because
  Tokio abort completion is asynchronous. Clean structural claims require the
  explicit retired and current join transactions.

### Active-B progress and final shutdown

- Retired A is moved out before its await, so the composition does not borrow
  or lock current B across A's close tail.
- The controller exposes a narrow ordered leg-control enqueue operation. It
  returns an opaque delivery receipt and does not expose the FIFO, seal, raw
  sender, or actor nursery. The real tracer proves positive delivery through B
  both while A is blocked in join and after A has cleared.
- Final current shutdown is monotonic:
  `Running -> ShuttingDown -> Joined`. `ShuttingDown` is published before the
  first `current.join().await`. Cancelling that caller cannot reopen session
  frames, ordered control, flush, recovery drive, standby registration,
  replacement, or retired-join mutation. The same current join can be retried
  to clean completion.

## Real Quinn tracer

`exact_clean_join_of_retired_a_clears_only_a_while_active_b_keeps_exchanging`
uses a localhost Quinn listener and three real actors A, B, and C. It does not
mint a test terminal, joined witness, physical identity, or memory transport.

The tracer proves:

1. A is really installed, B is really registered/attached, and controller plus
   actor ownership move A to retired and B to current.
2. Wrong-current and wrong-retired actors are rejected independently before
   cancellation. C deliberately reuses A's logical `LegId`, proving that
   logical equality cannot substitute for physical incarnation.
3. While A is blocked inside structural join, retired capacity stays occupied,
   a second cleanup task cannot start, and B completes an exact ordered
   leg-control delivery with a positive receipt.
4. A second B sentinel is queued before A clears, survives the clear, is
   decoded afterward, and completes the same positive ordered-delivery proof.
5. A clears with zero owned messages/bytes and no terminal failure; supervisor
   snapshot, current generation, route, phase, and B actor remain unchanged;
   stale repeated clear is rejected.
6. Cancelling final B join leaves the composition irreversibly
   `ShuttingDown`; all nine admission/mutation paths reject without changing
   the controller snapshot. Retrying the same join cleans both peers with zero
   ownership, and `Joined` still refuses later admission.

The exact tracer passed once and then passed **100/100** consecutive isolated
executions.

## Review repairs

The first GREEN was not accepted unchanged. Concentrated review found and TDD
closed four material proof gaps:

- The whole composition was initially borrowed across A's join, so B could
  not make progress; moving only A into the retained split-phase task repaired
  the ownership depth.
- The first wrong-actor check corrupted both current and retired conjuncts at
  once; independent current/retired corruptions now lock both pre-cancel
  checks, and a pre-clear B sentinel locks queue preservation.
- The first B probe used an ordinary Active-phase frame, not the ordered
  delivery lane. The accepted tracer uses real ordered leg-control plus the
  opaque positive delivery receipt.
- Cancelling final current join originally left the actor Closing while other
  controller mutations could still run. The composition-level monotonic phase
  now closes all nine admission/mutation entries before the first await.

Final independent identity/transaction, split-join/lifecycle, and stage-scope
reviews report unresolved P0/P1/P2 **0/0/0**.

## Local validation

- focused actor module: `47/47`;
- focused controller: `10/10`;
- focused `LegIo`: `28/28`;
- typed provenance: `129/129`;
- all targets/all features: library `1157 passed; 3 ignored`, binary `2/2`,
  concurrency `10/10; 4 ignored`, protocol `25/25`, session API `1/1`;
- four ignored concurrency sweeps: `4/4`;
- real-smoltcp adapter: `2/2`, with the core tracer repeated `100/100`;
- `cargo check` dev/release, release build, strict library Clippy, rustdoc,
  root rustfmt, and `git diff --check`: PASS;
- vendored Quinn with the exact local quinn-proto dependency:
  `47 passed; 3 ignored`, doctest `1/1`; vendored quinn-proto:
  `330/330`, doctests `3/3`;
- changed-secret and nested generated-artifact checks: PASS.

Existing vendored smoltcp and rustdoc warnings remain warnings; strict
mini_vpn library Clippy is clean. Scheduler-sensitive performance gates were
run without concurrent Cargo compilation.

## Remaining boundary and next step

The real TUN entry still selects `LegacyOwnedUpstream`. This slice drives the
production-shared outbound queue, but it does not add the typed actor raw-
inbound-to-controller bridge, a production session nursery/root, or full
bidirectional exchange. It also does not execute a real B-to-C second
replacement; it proves that the exact blocker is gone and capacity reports
reusable after A's structural clear.

Next close the rest of R6 through the same owner:

1. add the typed actor inbound-to-controller bridge without releasing inbound
   permits early;
2. drive a real A hard-close into exactly one B attach/replay transaction;
3. add silent-failure detection with explicit deadlines and no harness oracle;
4. close the checked recovery-horizon/capacity sum and production composition
   nursery lifecycle; and
5. re-run the bounded real-loopback R6 matrix and independent review.

That still does not cover R7 reverse-only switching, R8/R9, UDP, production
TUN, WAN, throughput, or M2. Do not start macOS TUN/VPS/M2 acceptance from
this result.

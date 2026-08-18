# Knife16 Two-Leg Foundation Local Results

Date: 2026-08-18

Status: **R1-R5 FOUNDATION PASS ONLY; R6-R9 NOT IMPLEMENTED;
TASK 4 NOT PASS; NO WAN/THROUGHPUT CLAIM; M3 BLOCKED**

## Outcome

Knife16 Task 4 R1-R5 now pass as a production-shared local foundation. The
accepted path crosses the real resumable codec, exact live-leg seal,
authenticated attach transaction, client and owner session supervisors,
typed source/replay ownership, `TargetIo`, bounded encoded transport, and one
global deterministic scheduler. The harness does not construct reducer
events, attach authority, ACK receipts, terminal authority, or switch facts.

This is not Task-4 acceptance. Standby registration, authenticated path
probes and reverse-only hints, the production-shared switch controller,
ordered acceptance-before-recovery, stale-A retirement, the R8 fault matrix,
and R9 real-smoltcp parity are not implemented. The public production entry
still selects the legacy adapter. No macOS TUN, VPS, WAN, or throughput run was
performed or authorized.

No D16, Endpoint, MTU, pool, QUIC window, chunk, Cubic, GSO, self-wake,
Knife15 workload, or quality threshold changed.

## R1-R5 closure

- R1 composes a lost `ATTACH_ACCEPTED` through authenticated generation
  status and a typed client catch-up capability without discarding live flows
  or replay.
- R2 binds owner attach and every post-attach DATA/ACK/CLOSE/RESET record to
  the exact process-local leg seal.
- R3 serializes proof verification, model preflight, authority CAS, model
  install, acceptance publication, and opaque recovery publication in one
  owner turn.
- R4 gives one supervisor sole reducer ownership and binds dequeued DATA to
  the exact reducer-minted `ReplayStored` receipt before returning effects.
- R5 carries one bidirectional TCP flow through encode, bounded byte
  transport, decode, exact-leg binding, both supervisors, `TargetIo`, partial
  and zero acceptance, application ACK, replay release, FIN, expiry, join,
  and zero final ownership.

## Irreversible-source admission

The session supervisor is the sole production mint for one TCP port factory.
It requires a healthy pristine model, derives session, role, direction,
per-flow and global byte/segment capacities from the reducer, and mints one
opaque origin. Factory clones share the same ledgers and monotonic flow gate;
the origin follows staging ownership into replay ownership.

A source reservation owns the ordered message slot, per-flow and global byte
permits, per-flow and global replay-segment permits, and offset headroom before
the Target or smoltcp extractor runs. DATA commits actual bytes without a
release/reacquire gap. EOF commits through the same pre-reserved FIFO slot, so
it cannot overtake DATA. Exact ACK or terminal capabilities release permits;
foreign factory provenance and reducer-capacity states that should be
unreachable after admission poison the supervisor and quarantine ownership.

## Reducer and Target transactions

Retryable global reducer pressure returns the exact `SessionEvent` or
`Driver::Control` command in its original lane for bounded retry. This covers
flow, receive-byte, receive-range, and terminal-tombstone admission. Local
DATA remains non-cloneable; its byte/segment admission makes replay-capacity
failure unreachable, and an invariant failure retains ownership while the
supervisor fails closed.

`TargetIo` distinguishes operational flow failure from adapter invariants.
Open, read, write, and half-close failures become typed protocol completions;
an invariant error permanently poisons the executor and retains the in-flight
input. A bounded pending-effect FIFO exposes exactly one `NeedsResume`
obligation. Post-I/O completions are retained across terminal-tombstone
pressure without repeating Target I/O. Session expiry force-discards even an
already graceful Target's adapter-owned buffers before exact join and bounded
tombstone retirement.

## Deterministic wire and work bounds

The wire scheduler processes one event per turn, freezes same-time
micro-round membership and phase order, uses checked counters, and fails
before mutation on ownership underflow. Adjacent reorder, duplicate, delay,
hold, drop, blackout, nested reorder, release, cancel, and join all account
for their physical envelopes. Fault scripts remain outside the controller-
visible transport interface.

The R5 scenario derives scheduler admission and the step ceiling only from
one manifest:

- turn caps: wire send `16`, wire delivery `16`, positive accept `4`,
  zero/would-block `2`, ACK construct `8`, ACK decode `8`, fixed actor `64`;
  total `118`;
- observed turns: `16/16/4/2/8/8/55`;
- byte caps: wire send `776`, wire delivery `776`, non-wire `6,656`; total
  `8,208`;
- observed bytes: `728/728/981`; total `2,437`.

Every category and each byte class has an independent checked ceiling, so one
class cannot borrow another's unused allowance. Accounting previews all
charges and commits only after scheduler admission; a failed schedule burns
neither budget nor ordinal. The reject-once transport probe creates no wire
ownership and consumes one additional fixed-actor retry only.

## Final conservation

The R5 finish gate requires the complete zero ledger, including:

- scheduler events/bytes, wire ready/held-reorder ownership, and outbound
  frames/encoded bytes;
- Target live flows, buffers, directives, joins, and tombstones;
- OwnerTarget pending effects, unique resume obligation, pending Target
  completions, pending writes/joins/read ownership, and aborted ownership;
- both reducers' replay/receive bytes, ranges, segments, flows, terminal
  tombstones, and session factory byte/segment permits;
- both supervisors' replay extents/bytes, poison flag, and quarantine
  extents/bytes; and
- the client flow port, sink actions, FIN, terminal, and driver queues.

## Review findings closed

Concentrated RED/GREEN and independent reviews closed the original scheduler,
reorder, counter, Target-failure, effect-continuation, and expiry findings,
plus later findings in source-segment admission, transient reducer-input
ownership, duplicate factory ledgers, factory provenance, supervisor poison/
quarantine, and per-category work accounting. Final R1-R5 reviews report
`P0=0` and `P1=0`.

One P2 is deliberately carried as an R6/R7 stop gate. The current owner attach
publication contains sibling non-cloneable acceptance and recovery values;
this proves at-most-once ownership but does not prove that acceptance entered
the ordered transport queue before recovery. Before any controller can use
the publication, recovery must consume a successful ordered-enqueue/send
receipt or both must enter one atomic ordered queue operation.

## Local gates

- owned-upstream focused tests: `179/179` PASS;
- `two_leg`: `32/32`; R5 `two_leg_harness`: `9/9` PASS;
- full root library: `1003/1003` PASS, `3` ignored; main: `2/2` PASS;
- typed leg/protocol provenance: `114/114` PASS;
- protocol integration: `19/19`; public session API: `1/1` PASS;
- concurrency harness: `10/10` PASS, `4` ignored;
- real-smoltcp Task-3 parity floor: `2/2` PASS and 100-repeat PASS;
- release all-target check: PASS;
- strict root library Clippy with the four documented legacy allowances:
  PASS; the all-target form still reports only the existing path-fixture
  unused/dead-code baseline after all Task-4 findings were removed;
- rustdoc: PASS with existing unrelated warnings;
- vendored Quinn with exact local quinn-proto: `40/40` PASS, `3` ignored,
  doctest `1/1`; vendored quinn-proto: `330/330`, docs `3/3` PASS;
- `cargo fmt --check`, `git diff --check`, provenance, and secret-shape scan:
  PASS.

## Next

Implement R6 P/A/L only: freeze `STANDBY_CONTROL_V1`, add five bounded
leg-control records, add the independent standby HMAC transcript, and mint
exact-leg standby/probe/hint capabilities. Then make the first controller
tracer consume a real transport-close capability and the typed ordered-send
barrier. The fault oracle must remain invisible to the controller.

Do not start Task 5, a production owner, real sockets, macOS TUN, VPS, WAN, or
throughput acceptance from this foundation result.

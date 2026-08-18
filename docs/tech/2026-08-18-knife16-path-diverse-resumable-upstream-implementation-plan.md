# Knife16 Path-Diverse Resumable Upstream Implementation Plan

Date: 2026-08-18

Status: **IN PROGRESS; TASKS 1–3 COMPLETE; TASK 4 R1–R5 FOUNDATION
ACCEPTED; R6 NEXT; M3 BLOCKED**

> Use `diagnose` and `tdd` for each behavior change. Use
> `improve-codebase-architecture` when a test cannot reach the required
> ownership seam, and `code-review` before every WAN run.

## Goal

Implement ADR-0015 by branch-by-abstraction: retain standard TUIC and the
proven local data plane while adding a server-owned, two-leg resumable
upstream. No long WAN test is authorized by a necessary-only partial stage.

## Task 1: Freeze Knife15 and protocol vocabulary — COMPLETE

- Publish the first Tier-B failure result and exact artifact hashes.
- Update ADR-0004 relationship through ADR-0015; retain compatibility TUIC.
- Add the session-owner, ingress-leg, replay-window, application-ACK, and UDP
  delivery vocabulary to `CONTEXT.md`.
- Update project memory and close same-class Knife15 reruns.
- Review, commit, and push.

Acceptance: one unambiguous current position says Knife15 failed, zero epoch
credit does not grant a retry, and M3 is blocked.

## Task 2: Protocol model and capacity characterization — COMPLETE

- Define versioned records, limits, errors, authentication binding, and state
  machines without I/O.
- Add RED/GREEN tests for offsets, partial ACK, duplicates, gaps, overlap,
  stale leg generation, final offsets, replay attach, and cross-session
  rejection.
- Characterize allocation and copy cost at 100/170/240 Mbit/s and choose the
  smallest per-flow/global bounds satisfying the accepted resume horizon.
- Prove integer overflow and malicious-length rejection.

Acceptance: pure tests establish all TCP invariants and bounded memory math;
no network or production adapter exists yet.

Result: PASS. The pure fixed core implements the bounded v1 codec, exact
request-correlated authenticated attach and generation resynchronization,
directional TCP ownership windows, stable cross-leg session capabilities,
two-phase FIN, bounded terminal tombstones, and checked replay/storage/copy
math. At a 500 ms total horizon, 100/170/240 Mbit/s require
6,250,000/10,625,000/15,000,000 application bytes per direction. Aggregate
capacity is derived independently from aggregate rate. Focused `94/94`, root
`807 + 3 ignored`, protocol `19/19`, public API `1/1`, harness
`819 + 3 ignored`, concurrency `10 + 4 ignored`, release, Clippy, rustdoc,
vendored Quinn/proto, fmt, diff, and two concentrated reviews pass with no
unresolved P0/P1. No network or production adapter exists yet. See
`docs/tech/2026-08-18-knife16-resumable-protocol-capacity-local-results.md`.

## Task 3: Deep `OwnedUpstream` abstraction

Status: **COMPLETE**

- Introduce a protocol-owned session/flow interface behind the current
  `ProxyUpstream` and `DatagramUpstream` call sites.
- Keep `TuicUpstream` and `FailoverUpstream` behavior unchanged.
- Separate session lifecycle, transport leg, TCP replay, and UDP delivery into
  modules with one owner each.
- Add adapter contract tests proving the old TUIC path remains byte-for-byte
  and lifecycle compatible.

Acceptance: TUN/smoltcp relay code selects either adapter without knowing
transport, leg, or replay mechanics. Architecture depth target: **9/10**.

Result: PASS for the Task-3 local seam. The object-safe facade preserves all
legacy relay variants and UDP behavior while a crate-private adapter-aware
event loop drives typed resumable flow ports through the real smoltcp path.
Session-global byte reservation precedes local extraction; application ACK is
minted only from exact `send_slice` acceptance; TLS-leg, replay, sink, FIN,
terminal, epoch, and uninstalled-open ownership are capability-bound. The
existing D16/local-egress actor remains the sole admission authority. Focused
`43/43`, all-target harness `875 + 3 ignored`, concurrency `10 + 4 ignored`,
typed provenance `101/101`, real fake-adapter `2/2` plus 100-repeat, release,
strict Clippy, rustdoc, vendored Quinn/proto, fmt, diff, and four concentrated
reviews pass with no unresolved P0/P1. Production two-leg transport, owner,
WAN, and throughput remain unimplemented. See
`docs/tech/2026-08-18-knife16-owned-upstream-local-results.md`.

## Task 4: Deterministic two-leg transport harness

Status: **IN PROGRESS — R1–R5 FOUNDATION ACCEPTED LOCALLY; R6–R9 OPEN**

- Build in-memory client/ingress/owner legs with deterministic clocks.
- Inject drop, reorder, duplicate, delayed ACK, primary blackouts from
  `300..800ms`, simultaneous attach, and stale-leg output.
- Assert one Target open, exact TCP bytes in both directions, no duplicated
  delivery, bounded replay, and unrelated-flow survival.
- Prove control and application ACK service cannot be starved by bulk replay.

Acceptance: every injected sub-second single-leg outage preserves exact TCP
delivery with no complete one-second receiver interruption. Unexpected REDs
stop for causal repair; timing constants are not tuned around them.

Foundation result: R1–R5 now pass locally through the production-shared
codec, exact leg seal, attach transaction, single-owner supervisor, typed
source/replay ownership, `TargetIo`, one-event wire scheduler, category-owned
work budgets, and a complete byte-level baseline. Source admission precedes
irreversible reads, transient reducer pressure returns exact inputs, one
pristine supervisor is the sole factory mint, and final continuation/replay/
Target/wire ownership is zero. Independent reviews report P0/P1 `0/0` for
this foundation.

This does not meet Task-4 acceptance. Standby registration, authenticated
probe/hint control, a production-shared switch controller, ordered
acceptance-before-recovery, stale-A retirement, the R8 fault matrix, and R9
real-smoltcp parity remain open. Result:
`docs/tech/2026-08-18-knife16-two-leg-foundation-local-results.md`.

## Task 5: UDP dual-leg delivery engine

- Implement packet sequence, bounded feedback window, deadline, dedup, and
  independent leg queues.
- Keep one active primary plus hot standby; allow only a bounded transition
  duplication window.
- Add deterministic saturation, reordering, duplicate, deadline, and
  primary-block tests.
- Publish exact per-boundary packet counters and queue/drop reasons.

Acceptance: blocking one leg cannot block the other; receiver delivery has no
duplicates; all losses are attributed to deadline, queue, transport, or
missing ingress evidence. Continuous 2x traffic is rejected by test/policy.

## Task 6: `mini_vpn-upstreamd` single owner

- Add one workspace server binary with authenticated session/attach and Target
  TCP/UDP socket ownership.
- Use one supervisor per session and structured cancellation per leg/flow.
- Apply bounded TCP backpressure and bounded UDP expiry; remove no bytes or
  tasks without terminal ownership evidence.
- Add real loopback Target tests for half-close, reset, resume grace, leg loss,
  and cleanup.

Acceptance: a leg may disappear without closing the Target TCP socket; owner
loss closes all owned flows deterministically after bounded grace; no task,
socket, replay byte, or queue byte leaks.

## Task 7: Local real-socket throughput gate

- Inventory exact uplink/downlink hot paths and instrument copies, scheduling,
  queues, application ACK, replay, and Endpoint ownership.
- Run 32MiB TCP forward/reverse plus the existing UDP profile through real
  loopback sockets and both logical legs.
- Inject one bounded transport blackout during bulk transfer.
- Run root/unit/integration/release/Clippy, vendored Quinn, D16, Endpoint,
  full-TUN, secret, and provenance gates.

Acceptance: TCP exceeds `170 Mbit/s`, exact payload and lifecycle pass, replay
stays within Task-2 bounds, and the blackout does not create a complete
one-second receiver interruption. Below `170 Mbit/s` is an architecture
failure; do not tune D16, Endpoint, MTU, pool, Cubic, GSO, or workload.

## Task 8: Security and abuse review

- Threat-model stolen resume tokens, replay, cross-session attach, leg
  downgrade, amplification, memory exhaustion, unauthenticated Target opens,
  and log secret leakage.
- Fuzz codec and state machines; cap sessions, flows, replay, UDP feedback, and
  attach attempts.
- Bind every evidence identity to protocol/server build and ingress route.

Acceptance: no unresolved P0/P1 security, ownership, or resource-exhaustion
finding.

## Task 9: Two-ingress bounded qualification

- Provision two materially independent client-to-ingress-to-owner paths to the
  same owner; prove both segments' provider/ASN/route independence rather than
  assuming it from ingress labels.
- Add server socket-overflow, receive/send queue, per-leg sequence, and
  application-ACK observer evidence.
- Run a short qualification with induced primary-leg loss and verify secondary
  ownership, exact paired capture, notification, and cleanup.

Acceptance: one path failure is recovered through the other without changing
the Target socket; packet/byte boundaries reconcile and all ownership cleans.
No 24-hour run occurs before this passes.

## Task 10: Knife16 long macOS acceptance

- Freeze source, binaries, protocol, owner, both ingress identities, routes,
  workload, and observer.
- Reuse the existing strict complete-interval semantics and UDP `<=3%` limit.
- Require TCP zero complete receiver-zero intervals, no consecutive outage,
  unchanged gap/DNS/real-client/D16/Endpoint/TUN gates, paired server evidence,
  notice, and cleanup.
- Record every switch and show whether the secondary path actually carried
  owned bytes/packets.

Acceptance: only a complete valid run can reopen M3. A genuine failure returns
to its exact architecture discriminator; it does not authorize threshold or
frozen-constant tuning.

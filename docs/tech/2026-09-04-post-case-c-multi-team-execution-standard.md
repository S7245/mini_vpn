# Post-Case-C Multi-Team Execution Standard

Status: **DRAFT / POST-CASE-C / NOT YET TRIGGERED**

This document defines how mini_vpn may use multiple engineering Agents after
the R7 Case C architecture contract is accepted. It is an execution standard,
not authorization to edit Rust, run external acceptance, switch public TUN, or
start R8/R9.

## 1. Purpose

The objective is to shorten the path from an accepted Case C architecture to a
real production TCP owner-server slice while preserving:

- one Authority for each lifecycle;
- deep Modules with small Interfaces;
- exact ownership on success, rejection, pressure, fatal, cancellation,
  shutdown, and modeled unwind;
- bounded capacity and backpressure;
- one Writer for every overlapping Module; and
- independent review before promotion.

Parallelism is used only where write sets and Authority are disjoint. The
critical ownership transaction remains sequential under one Integrator.

## 2. Trigger gate

The multi-team workflow may be triggered only when all of the following are
true:

1. The Case C owner-server-root architecture has an independent final verdict
   of `P0/P1/P2 = 0/0/0, Approve`.
2. `docs/current/STATUS.md` explicitly records that architecture acceptance and
   names the accepted evidence document.
3. No open Case C P0/P1 finding, ownership ambiguity, capacity-domain conflict,
   or false-zero branch remains.
4. The accepted contract still requires a real production parent, pre-auth
   custody, bounded session storage, fused whole-root adoption, and exact
   shutdown conversion; acceptance must not be inferred from a test-only root.
5. A separate implementation-planning slice is authorized to freeze exact
   files, symbols, visibility, Writer ownership, line budgets, TDD filters, and
   the stop rule.

The current trigger evidence is the Case C sidecar:

- [Case C owner-server-root architecture](2026-09-04-knife16-r7-case-c-owner-server-root-architecture.md)

File existence, a proposed status, a partial Reviewer approval, or one green
test does not satisfy this gate.

## 3. Team topology

### Integrator — contract and promotion owner

Owns:

- dependency order and the frozen Interface at every cross-team Seam;
- exact allowed-file and symbol assignments;
- integration sequencing and conflict resolution;
- code-growth inventory updates when a grandfathered file shrinks;
- current status/result promotion; and
- the final local and external readiness verdict.

The Integrator does not let two Writers modify the same Module concurrently and
does not silently repair a failed team slice.

### Team A — OwnerServerRoot Module

Owns the process-level lifecycle Authority:

- bounded pre-auth and authenticated session slots;
- phase and lease conservation;
- whole physical-root adoption and exact return;
- terminalization, `Reclaimable`, `NotJoined`, and whole-server zero; and
- bounded control scheduling and fairness.

Team A is the sole Writer for the root state machine and fused adoption
transaction. No other team may copy its state or publish a second lifecycle
Interface.

### Team B — Quinn ingress/bootstrap Adapter

Owns transport-facing admission Implementation:

- listener admission and bootstrap events;
- root-retained bootstrap/task/result custody before the first subsequent
  await or cancellation point;
- exact rejection and listener shutdown disposition; and
- translation into the frozen inward ingress/bootstrap Interface.

It does not authenticate sessions, own Target resources, or settle the server.

### Team C — TCP Target Adapter

Owns the real TCP socket Adapter for the existing `TargetIo` Interface:

- bounded connect, read, write, close, and error behavior;
- socket and pending-effect ownership;
- cancellation and shutdown disposition; and
- deterministic local Adapter tests.

It does not generalize `TargetIo` to UDP. UDP requires its own later accepted
Interface and is outside this workflow.

### Team D — deterministic verification and provenance

Owns disjoint test/provenance Modules only after production symbols are frozen:

- code-aware production-route and deletion tracers;
- slot, lease, tombstone, and false-zero conservation tests;
- success/rejection/pressure/fatal/cancellation/shutdown tracers;
- bounded scheduling/fairness evidence; and
- local integration harnesses that cross only accepted Interfaces.

Team D may detect a missing Interface but may not create or widen one.

### Independent Reviewer — read-only closure

Reviews every coherent slice for correctness, lifecycle ownership, capacity,
backpressure, Module Depth, Authority Locality, performance reachability, and
regression risk. Reviewer findings are not repaired by the Reviewer.

Recommended active topology is at most three disjoint Writers, one Integrator,
and one independent Reviewer. More parallel Writers require an explicit proof
that their Authority and write sets do not overlap.

## 4. Team work-packet contract

Before a Writer starts, the Integrator records:

1. goal and explicit non-goals;
2. owned Module and Authority;
3. exact allowed files and symbols;
4. frozen input/output Interface and ordering rules;
5. capacity and ownership invariants;
6. one expected focused RED and minimum GREEN;
7. required regressions and governance gates;
8. exact stop conditions; and
9. the report format, including modified files and exact line counts.

A work packet is invalid if it grants a shared Writer, relies on an unreviewed
public Interface, or describes cleanup without an exact owning value.

## 5. Fastest path 1–5

### 1. Freeze route, privacy, and team contracts

After the trigger gate, perform one architecture-to-implementation planning
slice. Select the real non-test owner-server Main route, private Module
placement, minimal visibility, concrete ingress/bootstrap Interface, bounded
capacity domains, and exact Writer assignments.

Exit: one reviewed plan with no Rust behavior change and no unresolved P0/P1.

### 2. Build three disjoint prerequisites in parallel

- Team A builds the bounded OwnerServerRoot storage/state skeleton and typed
  lease conservation without pretending the production path is complete.
- Team B builds the ownership-retaining pre-auth bootstrap Adapter against the
  frozen inward Interface.
- Team C builds the real TCP socket Adapter through `TargetIo`.
- Team D may prepare disjoint RED/provenance fixtures after symbol names and
  routes are frozen.

Exit: each Module is independently green through its own Interface, and no team
has introduced a second owner or cross-team callback protocol.

### 3. Fuse one production TCP vertical slice

Team A and the Integrator sequentially connect:

```text
non-test owner-server Main
  -> Quinn ingress/bootstrap Adapter
  -> root-owned pre-auth custody
  -> authentication and accepted seed
  -> exact-parts preflight
  -> fused whole-root adoption
  -> real TCP Target Adapter
  -> root-owned terminalization and NotJoined/JoinedZero conversion
```

All fallible reservations precede publication. The successful whole-root build
and parent-slot publication form the final infallible commit edge.

Exit: one real production-reachable TCP session completes or fails closed under
the same ownership ledger. This is necessary-only and does not enable UDP or
public TUN.

### 4. Close deterministic local acceptance

Run focused and repeated success, rejection, pressure, fatal, observer
cancellation, shutdown, and modeled-unwind tracers; capacity/fairness tests;
directly affected regressions; production-route and deletion provenance;
format/diff/check; code-growth governance; and independent review.

Exit: all required local gates are green, Reviewer reports `0/0/0, Approve`, and
`docs/current/STATUS.md` explicitly says the TCP production slice is ready for
an external gate.

### 5. Run one controlled VPS TCP acceptance

Only after Step 4 promotion, execute a one-shot VPS plan with exact source,
binary, configuration, endpoint, observability, timeout, cleanup, and
fail-closed verdict rules. Validate real QUIC/TUIC interoperability, TCP Target
reachability, lifecycle settlement, bounded backlog, and diagnostic counters.

Exit: record the external result. A failure returns to evidence-driven diagnosis;
it does not authorize UDP, public TUN, throughput claims, or repeated tuning.

## 6. Integration and repair rules

- Characterization Move, behavior change, and governance inventory update are
  separate coherent slices.
- Cross-team changes enter only through the frozen Interface; an Interface
  change returns to the Integrator before any Writer continues.
- Every unexpected compile/test failure stops that Writer. The team supplies a
  failure tree and waits for repair authorization.
- A second repair in one failure class deepens the responsible Module; a third
  local patch is forbidden by the structural convergence gate.
- The Integrator merges in dependency order and reruns affected seam tests; a
  team's isolated green result is not integration acceptance.
- No Agent may use copied inventory, `Arc`, `Option::None` ownership holes,
  abort, detach, leak, `Drop`, global registry, or a spawned whole-root task as
  settlement evidence.

## 7. Frozen non-goals

Until a later explicit gate, this workflow does not:

- implement or redesign UDP;
- switch the public TUN route;
- claim R7a-d, Task 4, R8, or R9 completion;
- run WAN, throughput, M2, or macOS TUN acceptance;
- generalize providers with speculative traits or dynamic dispatch;
- route DATA records through a global custody table or lock; or
- clean unrelated dirty-worktree changes.

## 8. Completion and next decision

This multi-team workflow is complete only when Steps 1–5 each own explicit
evidence. Completion proves one production-reachable TCP owner-server vertical
slice and its lifecycle; it does not by itself complete R7.

After the VPS result, the Integrator presents one decision:

1. repair a localized evidenced defect;
2. deepen the owning Module if the failure crosses Authorities;
3. proceed to the next R7 physical slice; or
4. separately design the UDP inward Interface.

No next decision is implied automatically.

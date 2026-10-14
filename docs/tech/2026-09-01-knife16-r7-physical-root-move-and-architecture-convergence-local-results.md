# Knife16 R7 physical-root Move and architecture convergence local results

Date: 2026-09-01.

Status: **TEST-ONLY MOVE/EXACT-CLEANUP AND ARCHITECTURE-ONLY CONVERGENCE
INDEPENDENTLY ACCEPTED; FIRST BEHAVIOR SLICE REQUIRES USER CONFIRMATION;
PHYSICAL R7a-d, TASK 4, PUBLIC TUN, WAN, THROUGHPUT, AND M2 ARE NOT ACCEPTED**.

## Outcome

This stage establishes a stable pre-implementation boundary for the remaining
physical R7 work.

The accepted code change is deliberately test-only:

- the B-only recovery composition moved out of the oversized owner Module into
  the private `owner/physical_root.rs` test adapter;
- scalar-equal and pressure tracers now settle every exact attach permit,
  arbiter, actor terminal, terminal stamp, join result, and listener outcome;
- no discarded `join_current_for_shutdown` result remains in those tracers;
- the adapter remains `#[cfg(test)]` and is not a future production root; and
- all focused, provenance, governance, format, diff, and library-check gates
  listed below are green with an independent Approve review.

The architecture audit then stopped behavior edits and found two promotion
blockers:

1. the necessary-only R7 scan manifest still budgets `2 * F`, while the
   production Authority correctly requires `2 * (F + Z)` because reusable live
   flow capacity and retained terminal-grace progress can coexist; and
2. the current controller/output exact-zero predicate omits
   `snapshot.progress_entries`, so it can report zero while reverse-progress
   ownership is live.

The production physical root is also not reachable. The accepted initial seed
owns current A, its exact initial handoff, and the sole checked controller, but
has no production consumer. The existing B-only adapter owns neither current A
nor whole-session authority and must not be ungated.

No production behavior or test behavior was changed by the architecture-only
part of this stage.

## Accepted Move and exact-cleanup evidence

Exact line ratchets at the accepted boundary are:

| Path | Lines | Classification |
|---|---:|---|
| `src/owned_upstream/quinn_leg_composition/owner.rs` | `2062` | grandfathered production, shrink-only |
| `src/owned_upstream/quinn_leg_composition/owner/physical_root.rs` | `702` | private test-only adapter |
| `src/owned_upstream/quinn_leg_actor/tests/r7_physical_attach_permit.rs` | `1407` | grandfathered test leaf, shrink-only |
| `src/owned_upstream/quinn_leg_actor/tests/r7_physical_attach_permit/cleanup.rs` | `195` | private test child |

Validation at that boundary:

| Gate | Result |
|---|---:|
| Physical attach permit | `9/9` |
| Legacy B-only recovery | `2/2` |
| Terminal transaction | `5/5` |
| Knife16 provenance | `158/158` |
| Standalone code-growth governance | `6/6` |
| Cargo all-features code-growth governance | `6/6` |
| Repository test-Module provenance | `6/6` |
| `cargo fmt --all -- --check` | PASS |
| `git diff --check` | PASS |
| `cargo check --lib` | PASS; established vendored-smoltcp warnings only |
| Independent exact-cleanup re-review | P0/P1/P2 `0/0/0`; Approve |

This evidence closes the test-fixture cleanup finding only. It does not prove a
production root, a production terminal transaction, or whole-R7 exact zero.

## Architecture assessment

The current physical-R7 structure is **5/10** for convergence readiness. Typed
initial ownership, actor typestates, exact attach permits, and the
controller/output transaction skeleton provide strong local seams. Authority
Locality remains incomplete because current A, B, controller/output, progress,
probe/hint, terminal, endpoint, and session settlement do not yet live in one
production root. The target after the plan below is **9/10**: one deep private
Module, one closed topology, one terminal transaction, and one checked ledger;
the remaining point is reserved for later production/public reachability and
external acceptance rather than claimed statically.

The design applies the architecture gates as follows:

- **Module Depth:** put physical ownership in one deep private Module, not a
  shallow wrapper around the test adapter.
- **Interface:** accept only consuming typed seeds/transactions and root-owned
  drive tokens; expose no raw controller errors, actor parts, caller-selected
  hint facts, callbacks, or wide parameter lists.
- **Authority Locality:** every normal settlement is a single-root transaction;
  unknown interruption stays in root-owned sticky quarantine.
- **Branch by abstraction:** move the hard-terminal Implementation before
  changing it, then replace one production branch at a time and delete or
  privatize the old branch.
- **Single-partition consistency:** synchronous control-path mutation occurs
  under one exclusive `&mut` root; no new global lock, dynamic dispatch,
  per-record allocation, or DATA-hot-path inventory is introduced.
- **Fault tolerance:** safety wins over liveness. A waiter may lose observation,
  but cannot lose the root-owned join/settlement transaction.

## Falsifiable failure tree

Each branch names a prediction that a deterministic tracer can falsify.

### A. Capacity and scan work

- **A1 — terminal-grace undercount.** With `F = 16`, `Z = 1`, and scan quantum
  `Q = 32`, `2 * F` predicts one turn while the sound `2 * (F + Z) = 34`
  requires two. A three-episode manifest must therefore reserve six turns.
- **A2 — addition overflow.** `F = usize::MAX`, `Z = 1` must reject before a
  manifest or root exists.
- **A3 — direction multiplication overflow.** A representable `F + Z` whose
  doubling overflows must reject at the second checked operation.
- **A4 — episode multiplication overflow.** A representable direction count
  and scan-turn count whose multiplication by episodes overflows must reject.
- **A5 — revision-reset undercount.** A successful progress mutation resets an
  in-flight cursor. Every such reset in a finite acceptance scenario must
  consume one declared restart credit; total attempts are completed scans plus
  resets. An undeclared reset fails the scenario closed without pretending the
  original turn bound still applies.
- **A6 — false zero.** A controller snapshot with one correlated terminal
  progress entry must make controller/output zero false; exact terminal-grace
  retirement must remove the correlated progress, tombstone, and timer before
  zero can become true.

### B. Production construction and physical topology

- **B1 — unreachable seed.** Source provenance currently finds no production
  consumer of `QuinnOwnerInitialPhysicalRootSeed<T>`; the first root slice must
  create exactly one and leave the public TUN route unchanged.
- **B2 — B-only false root.** Ungating `QuinnOwnerRecoverySessionComposition<T>`
  would leave current A, active handoff, endpoint, and session outside. A source
  ratchet must keep the adapter and all callers test-only.
- **B3 — split install.** Wrong clock, runtime, FIFO, seal, or standby identity
  must return the exact seed/build transaction unchanged before any actor FIFO
  mutation. Before `OwnerStandbySlot::admit`, all A/B/candidate ownership must
  already be stored in a stable, caller-owned `BuildingCandidate` physical root
  state with an in-root guard. If mutation begins and exact rollback cannot be
  proved, that root remains sticky non-zero quarantine.
- **B4 — invalid actor combinations.** The type system must prevent a standby
  without its slot/FIFO, `active_b` without its topology transition, a joined
  actor represented as live, or more than two physical actors.

### C. Controller/output and hard terminal

- **C1 — raw error escape.** A controller failure returned as a raw error can be
  dropped before root settlement. The production entry must instead store the
  exact failure in the root and return a closed value-only outcome.
- **C2 — post-bind unwind.** A panic after controller terminal bind but before
  bound/stamp staging must leave one visible transaction/residual/stamp and
  sticky non-zero quarantine; re-entry must not bind or discard again.
- **C3 — early publication.** Live page, drained marker, rejected page, retry,
  lifecycle, progress, or terminal high-water must prevent `Stopped` from being
  published until its exact no-ACK settlement has committed.
- **C4 — lost guard.** A preflight rejection before any root mutation returns
  the exact input and leaves Live bit-identical. Once admission seal and first
  outcome commit atomically into root-owned `Terminalizing::Prepared`, Drop
  before controller entry retains that same staged non-zero transaction; it
  cannot restore Live. Drop or `mem::forget` after controller entry remains in
  its known staged state or sticky quarantine and non-zero.
- **C5 — split first cause.** Every permutation of fatal, cancellation, and
  shutdown must retain the first committed root outcome and install all actor
  join ownership before any await.

### D. Physical attach and arbitration

- **D1 — one-way consistency.** The only legal attach states are all-false and
  all-true for slot, `Accepted` stage, `AcceptedPendingCommit` marker, and
  actor-held acceptance. Any mixed state is quarantine, never zero.
- **D2 — rejection/pressure loss.** Rejection returns the same permit/frame;
  pressure retains exactly one transaction and the same stored/independent
  bytes. Retry cannot mint a second nonce or ACK.
- **D3 — hard upgrade duplication.** A hard failure arriving after a soft hint
  or local-stall reservation upgrades the same epoch/nonce/ATTACH and cannot
  create a second generation.

### E. Probe, hint, and no-oracle evidence

- **E1 — fabricated evidence.** The harness may schedule bytes, close, time,
  pressure, and Target behavior, but cannot inspect a fault script, call a raw
  switch, fabricate reducer observations, or mint a hint.
- **E2 — wrong completion signal.** QUIC ACK, queue admission, or physical
  delivery ACK cannot close replay recovery; only a positive reducer-committed
  B application ACK intersecting the exact replay range can do so.
- **E3 — local-zero masking.** Controller/output zero while actor, FIFO, replay,
  timer, endpoint, or session authority remains live must make whole-root zero
  false.

## Capacity reconciliation

Let:

```text
F = max_live_flows
Z = max_terminal_tombstones
T = checked_add(F, Z)
N = checked_mul(T, 2)
Q = OWNER_REVERSE_SCAN_QUANTUM
turns_per_attempt = ceil(N / Q)
S = max_completed_scan_episodes
R = max_progress_revision_resets_while_scanning
A = checked_add(S, R)
max_scan_turns = checked_mul(turns_per_attempt, A)
```

`F` and `Z` are independent bounded populations. A terminal flow releases its
live-flow slot while its direction progress can remain until terminal-grace
retirement, so borrowing `Z` from `F` is unsound. The production
`OwnerProgressCapacity::checked(F, Z)` already owns the correct checked
derivation. `R7OwnerScanCapacity` must consume that typed Authority rather than
re-derive a scalar live-only bound.

`N` is the width of one stable-revision attempt, not a lifetime work bound.
`OwnerReverseProgress` resets its cursor after every successful revision
mutation. The finite no-oracle scenario must therefore declare and enforce a
separate `R` from its bounded reducer-observation/mutation schedule. The first
attempt for each desired completed scan consumes `S`; each observed
revision-invalidated restart consumes one of `R`. The harness rejects a
scheduled mutating observation that would exceed `R`, before performing it, or
retains a typed non-zero capacity failure; it never silently grants another
attempt. While a cursor is live, a restart credit is reserved before entering a
progress mutation, returned unchanged if the mutation rejects or leaves the
revision unchanged, and committed only when the revision advances. Production
retains only the per-root-turn `Q` work bound and a
resumable cursor—`max_scan_turns` is an acceptance-scenario bound, not a claim
that an indefinitely mutating session has finite lifetime work.

Required deterministic values are:

| Inputs | Direction entries |
|---|---:|
| `F=1, Z=3` | `8` |
| `F=4, Z=9` | `26` |
| `F=16, Z=1, Q=32` | `34`, two turns per stable-revision attempt |

For `F=16`, `Z=1`, `S=3`, and `R=2`, the checked scenario bound is five
attempts and ten scan turns. Required failures include overflow in `F + Z`,
`* 2`, `S + R`, and the final turn multiplication. A reset after the first
32-entry chunk must charge exactly one restart; a second reset with `R=1` is
rejected without mutation.

The dynamic correlation is:

```text
P_live + P_terminal == P_total == progress.len()
P_live <= 2 * live_flows
P_terminal <= 2 * live_terminal_tombstones
P_total <= 2 * (F + Z)
```

Every progress key must correlate to exactly one reducer live direction or one
terminal tombstone plus its exact terminal-grace timer/action. Duplicate or
uncorrelated entries are sticky audit failure.

## Whole-R7 ownership and capacity ledger

There is deliberately no production-global root spanning two endpoints. The
owner and client each have one Authority-local checked root ledger and settle
only their own endpoint/session. The deterministic no-oracle harness owns a
third transit ledger and computes a read-only checked aggregate from immutable
snapshots. It cannot mutate either endpoint through that aggregate.

Independent stored and recomputed byte sums are retained for every byte-owning
domain.

| Domain | Live inventory and bound | Required correlation/settlement |
|---|---|---|
| Physical topology | current/standby/retired/joining actors; `actors <= 2` | closed enum owns actor + exact handoff/slot/FIFO together |
| Attach | slot, stage, marker, held acceptance, permit, frame, nonce/epoch | bidirectional all-false/all-true invariant; exact identity across all-true |
| Controller | pending/retained continuation, retained output/observation, retry, failure | one in-flight transaction; raw failure never escapes |
| Output | page/drain/rejected messages, independently summed bytes, stored bytes | `live_page + rejected_page <= 1`; front identity stable under pressure |
| Continuation | page bit, drained marker, rejected-page bit, controller pending | sums exactly to controller pending and is at most one |
| Lifecycle | port, transit, typed actions | `port + transit <= 1`; actions bounded by `Z + 3` |
| Progress | live/terminal direction entries and cursor | exact live-or-tombstone correlation; bound `2 * (F + Z)` |
| Replay/session | extents, segments, stored/owned bytes, tombstones | reducer is sole mutation owner; positive application ACK releases bytes |
| Probe/hint | probe timer/response/freshness, pending/admitted hint, sequence | each fixed slot at most one; exact B incarnation and freshness |
| Arbiter | first trigger, epoch, nonce, pending ATTACH, hard-upgrade state | one epoch/nonce/transaction/generation across all trigger orderings |
| Messages/bytes | actor channels/FIFOs, ordered wait, root FIFO, controller turns, replay, pending control, driver port, quarantine, terminal/shutdown residual | checked independent sum equals stored admission bytes per domain |
| ACKs | application observations, actor DELIVERY ACK pending/emitted/discarded | application ACK only from reducer commit; DELIVERY ACK only after durable root custody; no-ACK terminal emits zero |
| Timers | hard, local ACK stall, current/B probe, aggregate reverse stall, terminal grace | each timer owned by one typed root slot and correlated episode/incarnation |
| Terminal | first outcome, transaction, residual, stamp, high-water, quarantine | first outcome frozen; live residuals consumed before final publication |
| Join/endpoint/session | join tasks/results, endpoint driver, endpoint and sole session Authority, shutdown admission | joins installed before await; all zero before `Joined` audit |
| Client root | client A/B actors/FIFOs, classified control, ordered wait, physical hint validator, arbiter, pending ATTACH, client timers, endpoint/session | one client Authority-local ledger; cannot be owned or settled by the owner root |
| Harness transit | wire scheduler/events, in-flight encoded records, fault schedule, Target/sink queues and accepted bytes | no-oracle harness owns and independently sums these; all zero before aggregate acceptance |

Each endpoint's `root_live_exact_zero` requires every locally owned live count,
independently summed byte, stored byte, ACK, timer, transaction, actor,
endpoint, and session Authority to be zero; every correlation must be valid;
and audit failure/quarantine must be absent. The harness accepts
`whole_r7_exact_zero` only when owner root, client root, and transit/Target/sink
ledgers are each exact zero and their cross-endpoint message/byte conservation
equations balance. Bounded immutable copy-only settlement history may remain
and is not added back into live ownership.

### Six normal settlements

| Class | Exact terminal condition |
|---|---|
| Success | one committed generation; losers settled; replay closed by positive B application ACK; both endpoint ledgers and transit ledger zero at final cleanup |
| Rejection | rejecting endpoint snapshot bit-identical; exact input/permit returned; no nonce/ACK/generation; both roots and transit then settle to zero |
| Pressure | exactly one retained transaction and matching bytes in its owning root or transit queue; the same value retries; aggregate final settlement reaches zero |
| Fatal | first fatal outcome frozen in the affected root; no-ACK discard; both roots' actors joined; transit/Target/sink zero |
| Cancellation | cancellation cannot rewrite a prior fatal; waiter loss cannot drop either root's join ownership; aggregate final zero |
| Shutdown | each root seals admission and runs its dedicated no-ACK transaction; transit drains/discards exactly; aggregate final zero |

Unknown panic, unwind after an irreversible mutation, or audit corruption is
not a seventh zero case. It must end in sticky, observable, non-zero quarantine.

## Production root design

Add one private deep Module:

```text
quinn_leg_composition::owner::physical_session_root
  OwnerPhysicalSessionRoot<T>
  OwnerPhysicalTopology<T>
  OwnerPhysicalRootBuildTransaction<T>
  OwnerPhysicalTerminalTransaction<'_, T>
  whole-root inventory and immutable terminal audit
```

The Module owns what no current object owns together:

- one closed physical topology;
- exactly one controller/output root, and therefore the sole
  controller -> Target executor -> supervisor/session Authority chain;
- current, standby, retired, and joining actor ownership with exact handoff,
  slot, FIFO, endpoint, and incarnation bindings;
- progress, probe/freshness, authenticated hint, and arbiter transactions; and
- first root outcome, shutdown admission, terminal settlement, joins,
  quarantine, and the whole-root ledger.

The topology is an enum, not independent `Option`s:

```text
CurrentOnly { current }
CurrentWithAuthenticatedCandidate { current, candidate_actor, candidate }
InstallingCandidate { current, candidate_actor, prepared, stage }
CurrentWithStandby { current, standby }
Switching { current, standby, accepted_attach }
CurrentWithRetired { current, retired }
Legless { high_water }
Settling { actors_and_join_tasks }
Joined { immutable_audit }
Quarantined { known_inventory, cause }
```

The first branch-by-abstraction constructors are consuming:

- `from_initial_seed(QuinnOwnerInitialPhysicalRootSeed<T>, checked_witnesses)`
  creates `CurrentOnly`; rejection returns the same intact seed.
- `from_initial_authenticated_standby(
  QuinnOwnerInitialAuthenticatedStandby<T>, StartedQuinnLegActor,
  checked_witnesses)` consumes the existing one-shot A+B authentication
  transaction and exact B actor, performs read-only identity/capacity checks,
  and returns a stable `CurrentWithAuthenticatedCandidate` root. It does not
  admit to the B FIFO. Rejection returns one intact build transaction, not
  decomposed owners.

Before returning the stable root, the constructor verifies the shared clock,
runtime/IO/FIFO identity, initial handoff, B seal/binding, candidate queue,
capacity witness, and zero unsettled initial acceptance. A later root-owned
`InstallingCandidate` transaction is installed in the already-live root before
`OwnerStandbySlot::admit` performs the first FIFO mutation. Its guard owns every
A/B/candidate value and marks unknown unwind sticky non-zero; successful admit
is immediately stored in that state, and the final
`CurrentWithStandby` topology publication is an infallible move. There is no
window where an admitted candidate exists before a stable root Authority.

The current `OwnerStandbySlot::admit` allocates hint-registration state after
the queue push, so it cannot be used unchanged as that commit primitive. The B
slice must first split it into root-borrowed read-only preparation and a commit:
encoding/validation, all `Arc` allocation/cloning, byte/capacity checks, and the
prepared registered-standby value occur before queue admission. The bounded
queue push is the final fallible operation. Pressure returns the same prepared
transaction to `InstallingCandidate`; after a receipt is returned, only
non-allocating field moves into the root and the final topology transition are
allowed. A queue-internal panic after mutation leaves the root-owned actor FIFO
and `InstallingCandidate` visibly non-zero; task-level loss is retained by the
parent join Authority and cannot mint zero.

A later slice moves B authentication itself behind the already-live root so the
harness cannot choose topology; the one-shot authenticated value is only the
migration Adapter.

The test-only `QuinnOwnerRecoverySessionComposition<T>` remains gated. It is
never re-exported as production and never wrapped as the new root.

## Production-shared hard-terminal transaction

The current test-only transaction proves exact preservation, not exact zero.
Production has only an `Awaiting` placeholder; success still retains the exact
discarded terminal, actor stamp, and one controller legless high-water. Those
are correct live recovery Authorities until actor join or the next attach, so
calling that state zero would be false.

The production terminal state replaces the placeholder with closed semantics:

```text
Live
Terminalizing { first_outcome, exact_residual }
Stopped { first_outcome, controller_output_zero, immutable_audit }
Quarantined { first_outcome, exact_known_inventory, cause }
```

`OwnerControllerOutputTerminalTransaction<'_, T>` contains exactly one field:
`&mut QuinnOwnerControllerOutputRoot<T>`. The exact terminal envelope, stamp,
controller-bound value, controller failure, rejected output, lifecycle action,
and high-water are stored inside that root state, never on the caller stack or
in the transaction guard across a fallible controller call.

Its sequence is:

1. receive and validate the root-minted committed-outcome token without
   changing admission or storing it in the controller/output root;
2. without mutating the root, preflight page/drain/rejection/retry/lifecycle/
   progress byte and history capacity, including `progress_entries` and all
   correlations;
3. in one infallible root-state move, seal admission, freeze the first outcome,
   and install `Terminalizing::Prepared` with the exact terminal envelope/stamp;
4. bind the controller terminal; immediately store either the exact bound value
   or exact controller failure in root state;
5. no-ACK settle output/marker, rejected page, retry, lifecycle, and the
   controller terminal into `TerminalApplied`; and
6. retain the exact discarded terminal, stamp, and legless high-water as one
   correlated, visibly non-zero handoff to the physical-root finalizer.

The closed drive result is value-only:

```text
Applied(ControllerOutputTerminalApplied)
AlreadyApplied(ControllerOutputTerminalAppliedSummary)
TerminalRetained(ControllerOutputTerminalRetainedSummary)
ControllerFailed(ControllerOutputTerminalFailureSummary)
Quarantined(ControllerOutputQuarantineSummary)
```

No raw `OwnerSessionControllerError`, terminal witness, stamp, high-water,
page, or continuation can escape. `TerminalApplied` is exact ownership
preservation and is intentionally non-zero.

The first transaction that may truthfully publish hard-terminal exact zero is
`OwnerPhysicalTerminalTransaction<'_, T>`, not the controller/output subroot
alone. It owns the controller/output transaction as an internal stage, installs
all actor join tasks before any await, settles probe/hint/attach/FIFO/ACK and
endpoint/session ownership, verifies the joined terminal-stamp pair, then asks
the controller/output Authority to consume the exact discarded terminal,
stamp, legless high-water, terminal progress/tombstone, and grace timer. Only
after that consuming finalizer returns `ControllerOutputStoppedZero` and the
whole ledger is zero may the physical root publish `Joined`.

A waiter is never the transaction owner. All actor join tasks move into
`OwnerPhysicalTerminalTransaction` synchronously before an await. Fatal,
cancellation, and shutdown differ only in their committed first-outcome token;
they share the same settlement Implementation.

The transaction has three explicit interruption boundaries:

```text
ReadOnlyPreflight
  failure/drop -> Live bit-identical; exact input returned

Terminalizing::Prepared { admission sealed, first outcome, terminal custody }
  drop before controller entry -> same staged non-zero state; resumable only
  with the same root-owned outcome/custody

Terminalizing::Entered { exact known controller disposition or Unknown }
  known drop -> retain exact staged disposition
  unknown unwind/forget -> sticky non-zero quarantine
```

There is no separate mutable `admission_sealed` or `first_outcome` field whose
update can precede guard installation. Restoring `Awaiting` is legal only when
the read-only preflight never changed root state.

## Behavior-preserving extraction gate

`owner/continuation.rs` is already a shrink-only `1698`-line production file
and may not grow. Before terminal behavior changes, characterize the existing
source and move the complete terminal state/guard/Implementation into the
private child `owner/continuation/hard_terminal.rs`.

The Move is accepted only if:

- the public/crate-visible Interface and all behavior remain byte-for-byte or
  provenance-equivalent;
- the transaction remains one Authority-local `&mut root` object;
- no controller/output/stamp/error is split into a parameter or callback;
- no re-export, `pub(super)` surface, or pass-through method count grows merely
  to satisfy line limits; and
- focused characterization, provenance, governance, format, and diff gates are
  green before any `cfg` or ordering change.

If Rust privacy would require Interface sprawl, keep the transaction together
and deepen the root Module first. Movement and behavior repair are separate
green-to-green slices.

## Exact implementation sequence

Each numbered slice has one Writer for its overlapping Module. Read-only
reviewers may inspect any slice; writers may work in parallel only on disjoint
Modules.

1. **Capacity characterization Move.** Move the existing scan-capacity tests
   from the grandfathered `r7_capacity.rs` leaf into one private child so the
   parent shrinks. No assertions or behavior change.
2. **Capacity RED/GREEN.** Make the child tracers prove checked `F + Z`, `* 2`,
   scan ceiling, completed-scan plus revision-reset attempt accounting, final
   multiplication, saturation, and undeclared-reset rejection. Change only the
   existing harness derivation/episode-witness seams; consume typed
   `OwnerProgressCapacity` and keep production source net-zero or smaller.
3. **Progress false-zero RED/GREEN.** In a separate controller/output slice,
   behavior-preservingly shrink the grandfathered owner test index/leaf as
   needed, prove one retained correlated `progress_entries` value prevents
   subroot zero, and make the existing predicate's minimum net-zero correction.
4. **Hard-terminal Move.** Characterize and extract the complete existing
   terminal state/guard/Implementation from `continuation.rs` into the deep
   private child, with no behavior change.
5. **Production controller/output terminal RED/GREEN.** Remove the production
   `Awaiting` placeholder, unify controller failure/recovery/terminal Authority,
   close value-only outcomes, stage exact ownership before bind, settle live
   output/lifecycle, and prove `TerminalApplied` exact preservation plus sticky
   post-bind unwind. Do not call this subroot state exact zero.
6. **Current-only physical root.** Consume exactly one initial physical seed;
   prove current actor/handoff/controller/session correlation and exact
   constructor rejection/rollback.
7. **Authenticated B root branch.** Consume the one-shot authenticated standby
   and exact B actor into a stable candidate-holding root without FIFO mutation;
   first make standby admission prepare/commit with no allocation or fallible
   work after the queue receipt, then use an in-root `InstallingCandidate`
   guard to install actor + slot + FIFO atomically. Keep the old B-only adapter
   test-only.
8. **Whole-root hard-terminal exact-zero transaction.** Freeze first outcome,
   install join tasks before await, settle all controller/output and physical
   domains, consume the correlated stamp/high-water only after join, then prove
   fatal/cancellation/shutdown whole-root zero and panic quarantine.
9. **Physical attach/lifecycle convergence.** Enforce the bidirectional attach
   invariant and settle success/rejection/pressure/rollback through the same
   root transaction.
10. **Owner observation and B evidence.** Route reducer outstanding/terminal/
   application-ACK observations, deterministic progress scanning, real B probe
   response/freshness, and authenticated hint production through the root.
11. **Client composition.** Adapt physical hints into the accepted rejection
    kernel and route hard failure, local stall, and hint through one production
    arbiter with same-pending hard upgrade.
12. **No-oracle harness and promotion.** Prove R7a-d plus the six normal
    settlements, repeat races, run all local gates and independent review, and
    update current evidence. Public TUN and M2 remain separate later gates.

## First implementation slice boundary

The first behavior slice after user confirmation is capacity only.

The relevant frozen source counts are
`two_leg_harness.rs 3433`, `two_leg_harness/tests/mod.rs 496`, and
`two_leg_harness/tests/r7_capacity.rs 984`. The first two production/test-index
debts must not grow; the capacity leaf must shrink when its focused tests move.
The later false-zero slice separately respects `owner.rs 2062` and
`owner/tests/mod.rs 334`.

Allowed files/symbols:

- `src/owned_upstream/two_leg_harness.rs`:
  `R7OwnerScanCapacity::derive` and
  `R7ScenarioEpisodeBounds`/`R7NoOracleCapacityManifest::derive` scan-accounting
  fields only;
- `src/owned_upstream/two_leg_harness/tests/r7_capacity.rs`: movement and Module
  declaration only, with a strict net shrink;
- one new private child under
  `src/owned_upstream/two_leg_harness/tests/r7_capacity/`, at most `800` lines;
- repository provenance assertions only as a separate net-zero/shrinking
  source-ratchet change.

One Writer owns the harness/test Module. No Writer may edit `continuation.rs`,
`owner.rs`, `owner_initial.rs`, controller internals, the governance inventory,
or production physical-root Modules in this slice.

Acceptance is focused RED then GREEN for the width/restart/overflow cases,
followed by related capacity regressions, Knife16 provenance,
standalone and Cargo governance, repository test-Module provenance, fmt, diff,
and `cargo check --lib`. Any unexpected red stops all writing for causal
analysis and renewed user confirmation.

## Frozen non-goals and stop rules

- No production behavior or new tests are authorized by this architecture-only
  result itself.
- Do not switch public TUN or ungate the B-only adapter.
- Do not add UDP, owner server, second ingress, R8/R9, or incidental lifecycle
  functionality.
- Do not run macOS TUN, VPS, WAN, throughput, M2, or M3.
- Do not add line-debt inventory entries, lint allowances, source `include!`,
  escaping `#[path]`, global coordination, or DATA-hot-path audit work.
- Do not mix a behavior-preserving Move with behavior modification.
- A second same-class ownership/lifecycle/capacity failure triggers renewed
  structural review, not another local patch.
- Any new repair-test failure stops writing until evidence and a concrete
  minimum plan are reported and the user confirms the next attempt.
- Do not commit or push.

## Independent architecture review

The first independent read-only review returned **Request Changes**, P0/P1/P2
`0/5/1`. It found premature review status, a mixed first Writer slice,
revision-reset scan undercount, a cross-endpoint “one root” ledger, B admission
before stable root publication, and an imprecise pre-controller-entry Drop
rule.

This document has been revised to:

- keep implementation unauthorized and separate capacity from the later
  controller/output false-zero slice;
- charge completed scans plus every revision-invalidated restart with checked
  scenario arithmetic;
- use two Authority-local endpoint ledgers plus a read-only harness transit
  aggregate;
- publish a stable candidate-holding root before any B FIFO admission; and
- distinguish read-only preflight, committed terminal intent, and controller
  entry for Drop/unwind settlement.

The same Reviewer then re-read the reconciled files and returned **Approve**,
P0/P1/P2 `0/0/0`, with no new material finding. This accepts the
architecture-only convergence and its implementation order. It does not itself
authorize an implementation tracer: the user must still confirm the first
capacity-only slice.

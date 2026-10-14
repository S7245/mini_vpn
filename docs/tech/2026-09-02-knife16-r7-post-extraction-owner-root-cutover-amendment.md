# Knife16 R7 post-extraction owner physical-root cutover amendment

Date: 2026-09-02.

Status: **ARCHITECTURE-ONLY INDEPENDENTLY ACCEPTED; THE FIRST CURRENTONLY RUST
SLICE REMAINS SEPARATELY GATED, AND NO IMPLEMENTATION, PUBLIC ROUTE, OR EXTERNAL
ACCEPTANCE IS CLAIMED**.

## Scope and supersession

This amendment follows the hard-terminal extraction contract in
`2026-09-02-knife16-r7-hard-terminal-extraction-architecture-amendment.md` and
the accepted A-D implementation evidence in
`2026-09-02-knife16-r7-hard-terminal-extraction-local-results.md`. It narrowly
supersedes the post-extraction ordering in the **Exact implementation sequence**
of
`2026-09-01-knife16-r7-physical-root-move-and-architecture-convergence-local-results.md`.
It does not rewrite either document's historical evidence or extraction
contract.

The prior sequence placed production controller/output terminal behavior before
construction of the production physical root. That order is not implementable
under the already accepted Authority contract:

- the controller/output terminal transaction must consume a root-minted
  committed-outcome capability;
- only a production physical-root state transition may mint that capability;
  and
- the current `owner/physical_root.rs` is a `cfg(test)` B-only adapter, not the
  missing production Authority.

The post-extraction order is therefore revised to:

```text
CurrentOnly root Authority
  -> production terminal preservation
  -> root-owned no-ACK finalizer
```

This amendment changes ordering and ownership boundaries only. Physical R7a-d,
Task 4, runtime restart credit, authenticated B topology, public TUN, WAN,
throughput, and M2 remain incomplete.

## Grounded current state

The dated hard-terminal local result owns the evidence that the
behavior-preserving extraction is closed:

- `owner/continuation.rs` is `971` lines;
- private production child `owner/continuation/hard_terminal.rs` is `779`
  lines;
- the Knife16 provenance parent/child are `2988/787` lines;
- the code-growth governance file is `699` lines; and
- the independent closure reported P0/P1/P2 `0/0/0`, Approve, with scoped
  structural quality `10/10`.

That extraction preserved the current behavior. It did not install production
terminal behavior or a production physical root. In particular:

- `QuinnOwnerControllerOutputRoot<T>` remains the controller/output Authority
  anchor in `owner/continuation.rs`;
- the full terminal state machine and transaction in `hard_terminal.rs` remain
  test-gated while production retains only its `Awaiting` placeholder;
- no `OwnerPhysicalSessionRoot<T>`, `physical_session_root`,
  `from_initial_seed`, or production committed-outcome capability exists; and
- `owner/physical_root.rs` remains entirely test-only and explicitly disclaims
  being the future production root.

### Independent architecture review

The first independent review returned P0/P1/P2 `0/2/1`, Request Changes: the
constructor commit edge was too late, hard-terminal A-D lacked a dated evidence
owner, and the first RED named only a missing constructor rather than the whole
missing production root-plus-constructor seam. After those repairs, the same
Reviewer returned `0/1/0`, Request Changes, because the private initial seed can
be inspected only through reversible `into_parts`/`from_parts`, not through the
pre-split inspection order then described.

The final review of the corrected contract returned P0/P1/P2 `0/0/0`, Approve,
with Clean Architecture `10/10` and System Design `10/10`. Every finding above
is closed. This accepts only the architecture and ordering contract; it is not
evidence that the production root, constructor, terminal path, or no-ACK
finalizer has been implemented.

### Initial seed and cutover seams

`QuinnOwnerInitialPhysicalRootSeed<T>` is defined in
`owner_initial.rs`. It contains one current-A actor port—one
`StartedQuinnLegActor` plus its `OwnerTargetInitialHandoff`—and the sole
`OwnerSessionController<T>`, including that controller's Target executor.

The exact production path that creates it is:

```text
QuinnOwnerInitialSessionComposition::try_into_accepted_root_parts
  -> QuinnOwnerInitialAcceptedRootParts
  -> QuinnOwnerInitialAcceptedRootParts::into_physical_root_seed
  -> QuinnOwnerInitialPhysicalRootSeed<T>
```

The acceptance conversion rejects shutdown, unresolved acceptance, identity,
and same-clock mismatches before producing the accepted parts. The seed's
existing `into_parts`/`from_parts` pair is the only real seed inspection seam:
the fields are private and there is no non-consuming getter.
`seed.into_parts()` is an infallible, reversible ownership decomposition, not a
commit. It moves the exact actor, handoff, and controller into one local
constructor transaction. While that transaction still owns the controller,
`QuinnOwnerInitialPhysicalRootSeed::from_parts(actor, handoff, controller)` can
reconstruct the exact aggregate without clone, copy, scalar reconstruction, a
new getter, or Interface widening. After
`QuinnOwnerControllerOutputRoot::new(controller)` succeeds, the controller has
been consumed and the exact seed no longer exists or can be reconstructed.

The two current alternative exits are:

- `into_terminal_tracked_current`; and
- `into_initial_composition`.

They, together with the existing initial-A terminal, join, and shutdown
Implementation, are the future cutover/absorption seam. This amendment does not
change them. Until their production callers and Authority have been absorbed,
the first CurrentOnly constructor is a necessary-only root foothold, not proof
that a unique production Authority has already been reached.

The actor FIFO is not a separate seed field. It is encapsulated by the
`StartedQuinnLegActor`; the future root owns the exact FIFO through exclusive
ownership of that actor and may audit it only through the existing actor-owned
queue seam.

## Goal

Freeze one implementable route from the exact accepted initial seed to a single
production physical-session Authority that can later mint the only valid first
outcome, preserve it through the controller/output terminal stage, and finally
settle the whole root without ACK publication.

The completed cutover must make these statements true:

1. exactly one settled initial seed becomes exactly one CurrentOnly root;
2. that root owns current A, its handoff and actor-contained FIFO, the
   controller/output Authority, and all future first-outcome/shutdown/join
   state;
3. no second production initial-A terminal/shutdown Authority remains
   reachable;
4. terminal code can consume only an opaque root-minted capability, never a
   raw reason or test adapter; and
5. whole-root zero can be published only after every owned domain is settled.

## Non-goals

- No Rust, test, governance, archive, or historical architecture edit in this
  amendment slice.
- No production terminal state or ordering change yet.
- No authenticated B actor, slot, probe, freshness, hint, attach, switch, or
  arbiter wiring.
- No runtime restart-credit reserve/return/commit transaction.
- No public TUN switch, owner/server, second ingress, UDP, R8/R9, external run,
  WAN, throughput, M2, commit, or push.
- No claim that static capacity math, a constructor tracer, or the test-only
  B adapter completes physical R7.

## Authority, Interface, and Locality

### CurrentOnly root

The future production `OwnerPhysicalSessionRoot<T>::CurrentOnly` consumes and
only consumes one `QuinnOwnerInitialPhysicalRootSeed<T>`. Its closed state owns:

- the current-A `StartedQuinnLegActor`;
- the exact `OwnerTargetInitialHandoff`;
- the actor-contained current-A FIFO Authority;
- one `QuinnOwnerControllerOutputRoot<T>` constructed from the seed's sole
  controller and retaining its Target executor;
- structural shutdown/join custody; and
- an explicit runtime restart-credit state of `NotInstalled`.

The root must eventually absorb the existing initial-A terminal, join, and
shutdown Implementation and route the two alternative exits through the same
Authority. Only after that caller cutover may it be described as the unique
production first-outcome/shutdown/join Authority. A parallel terminal tracker,
join owner, shutdown composition, or token issuer is forbidden.

### Root-minted committed outcome

A committed outcome is an owning capability minted only by an infallible root
state transition that freezes the first outcome. It is private, non-`Clone`,
non-`Copy`, and cannot be constructed from a raw reason, generation, boolean,
or scalar counter.

`continuation::hard_terminal` may consume only an opaque inward port or this
owning capability. It must not:

- name or depend on the concrete physical-root type;
- import or call the `cfg(test)` B-only adapter;
- own the first-outcome state;
- mint, clone, cache, or reconstruct the capability; or
- accept actor, FIFO, slot, probe, timer, endpoint, or session Authorities as
  callback or parameter sprawl.

The controller/output transaction remains an internal stage of the root-owned
terminal transaction. Deleting the future physical root must eliminate the
capability producer. Deleting `hard_terminal` must eliminate the terminal
consumer while leaving root construction intact. Neither side may recreate the
other through a pass-through Interface.

## State and ownership model

The minimum production-root progression is:

```text
Exact initial seed
  -> LocalConstructor { exact actor, handoff, controller }
       reversible decomposition; not committed
  -> CurrentOnly { current A, handoff, actor FIFO,
                   controller/output root,
                   restart_credit = NotInstalled }
  -> Terminalizing { root-minted first outcome, exact known inventory }
  -> Joined { immutable audit, whole-root live ownership = 0 }

Any unknown post-commit disposition
  -> Quarantined { sticky nonzero or exact known inventory }
```

Construction is a closed transaction. It first performs the infallible,
reversible `seed.into_parts()` decomposition and stores the exact actor, handoff,
and controller in one local transaction. Only then does it perform every
seed-specific correlation, clock, runtime, actor-owned FIFO, checked capacity,
inventory, acceptance, and destination-state validation on those exact parts.
`QuinnOwnerControllerOutputRoot::new(controller)` is the final fallible
operation. Its success and the immediate infallible move/publication of the
actor, handoff, output root, and `NotInstalled` state form one fused commit edge.

There is no rollback window after a successful `new`: the controller has moved
into the output root and the exact seed has ceased to exist. No await,
allocation, fallible work, callback, logging side effect, additional validation,
or cancellable observation may occur between `Ok(output_root)` and the closed
CurrentOnly move/publication. Any state after that edge is owned by the root or
its sticky quarantine; it is never reconstructed as a seed.

The bidirectional construction invariant is:

```text
published CurrentOnly
  <=> current actor owned
  <=> handoff owned
  <=> actor-contained FIFO Authority owned
  <=> controller/output root owned
  <=> restart credit is explicitly NotInstalled
  <=> exact initial seed has been consumed once
```

No partially published root, detached seed part, duplicate controller, or
implicit restart-credit zero is valid.

## Commit, rollback, and quarantine

### Constructor preparation

The constructor first consumes the seed with `seed.into_parts()` and installs
the exact actor, handoff, and controller in one local constructor transaction.
This decomposition is infallible and reversible; it is not commit and exposes no
new Interface. Using the real parts is the only permitted inspection seam.

After decomposition and before consuming the controller, the constructor must:

- validate the actor/handoff/controller/session correlation;
- validate the shared-clock identity and settled acceptance evidence;
- validate checked capacity and inventory arithmetic;
- validate runtime and the actor-owned FIFO state;
- prove the CurrentOnly destination can hold every part; and
- preconstruct every value needed for publication without post-success
  allocation, callback, logging side effect, or fallible work.

Every modeled precommit validation rejection or pressure branch consumes the
local transaction and calls
`QuinnOwnerInitialPhysicalRootSeed::from_parts(actor, handoff, controller)` to
return the exact seed. Clone, copy, scalar reconstruction, a new inspection
getter, and Interface widening are forbidden.

After those checks, the transaction calls
`QuinnOwnerControllerOutputRoot::new(controller)` as the final fallible step. On
`Err`, `QuinnOwnerControllerOutputAdoptFailure::into_parts` returns the same
controller, and `QuinnOwnerInitialPhysicalRootSeed::from_parts` reconstructs the
exact seed with the same actor and handoff. Rejection or pressure publishes no
root, queue record, ACK, nonce, timer, or partial audit.

On `Ok(output_root)`, exact seed reconstruction is impossible and must not be
claimed. The success arm immediately performs the infallible CurrentOnly
move/publication; that fused success edge is the commit point.

### First outcome and terminal entry

Only a CurrentOnly root state transition may freeze the first outcome and mint
the committed capability. That transition is infallible once entered. Fatal,
cancellation, and shutdown may propose different outcomes, but the first
committed outcome wins exactly once; later proposals observe it and cannot
overwrite it.

All join tasks move synchronously into root custody before the first await. A
cancelled waiter loses only observation; it never drops the actor, FIFO,
terminal transaction, endpoint, or session Authority.

### Unknown interruption

Exact seed return is promised only for modeled `Result` rejection and pressure
branches, including the final constructor's `Err` arm. The synchronous
precommit path contains no await or panic-capable work; this contract does not
claim that panic or unwind can return a seed. The `Ok` arm has
no cancellable gap before CurrentOnly publication. After that fused commit edge,
unknown `Drop`, unwind, or forgotten-guard disposition must leave root-owned
exact known nonzero inventory or sticky quarantine. It must never reconstruct a
seed, restore a clean CurrentOnly state, manufacture a replacement capability,
or normalize uncertainty to exact zero.

## Runtime restart-credit contract

CurrentOnly begins with the explicit state `NotInstalled`; numeric zero is not
an equivalent representation. The future root-owned typestate is frozen as:

```text
NotInstalled
Finite
MutationReserved
RestartCommitted
Quarantined
```

The accounting equation

```text
available + reserved + committed == R
```

is an audit invariant only. Authority comes from a non-copyable credit/lease,
not from the values in that equation. Reserve must precede mutation; rejection
or no revision change returns the exact lease; a successful revision advance
alone commits it. Unknown mutation disposition quarantines the lease.

Runtime credit may be installed only after the same physical root owns both:

- controller progress, currently inside `owner_session_controller.rs`; and
- `OwnerSwitchHintIssuer`, currently an independent Authority in
  `reverse_switch.rs`.

Those Authorities are presently split. A naked counter, copied manifest,
caller-supplied reset ordinal, or test helper cannot bridge them and is not
production credit evidence.

## Capacity and backpressure

The already accepted static necessary-only capacity math remains unchanged:

```text
N = 2 * (F + Z)                         checked add and multiply
Q = 32
turns_per_attempt = ceil(N / Q)
A = S + R                               checked add
max_scan_turns = turns_per_attempt * A  checked multiply
```

For `F=16`, `Z=1`, `S=3`, and `R=2`, the exact witness is:

```text
N = 34 direction entries
turns_per_attempt = 2
A = 5 attempts
max_scan_turns = 10
```

This bounds static owner scanning; it does not grant runtime credits. Queue,
FIFO, replay, ACK, timer, join, endpoint, and session capacities remain explicit
root-ledger domains. No constructor or terminal step may use saturation,
default capacity, unbounded buffering, post-commit allocation, or work hidden
outside the root.

## Falsifiable failure tree

### Success

- Exactly one settled seed is consumed.
- CurrentOnly owns the actor, handoff, actor-contained FIFO,
  controller/output root and Target executor, and structural shutdown/join
  custody.
- Seed parts and their message/byte inventory are preserved exactly; no second
  owner remains reachable.

Falsifier: any seed part can be observed both outside and inside the root, the
FIFO is detached from its actor, or a second controller/terminal owner exists.

### Rejection

- After reversible decomposition, every modeled clock, runtime, FIFO, capacity,
  identity, or unresolved-acceptance mismatch returns the exact seed through
  `from_parts(actor, handoff, controller)`.
- No root is published and no first-outcome capability can be minted.

Falsifier: rejection drops/rebuilds a seed part from scalar identity, changes
inventory, or leaves a partially reachable root.

### Pressure

- All fallible capacity and destination work occurs before commit.
- Every modeled precommit pressure branch reassembles the exact seed from the
  same actor, handoff, and controller.
- Queue/FIFO ownership, ACK count, nonce, timer, and seed identity remain
  unchanged on pressure.

Falsifier: pressure occurs after root publication or leaks a queue entry, ACK,
nonce, timer, allocation, or partial controller/output state.

### Fatal

- Without a root-minted committed-outcome capability, terminal entry is
  impossible.
- With it, the first outcome freezes once and raw controller/transport errors do
  not escape the closed value-only boundary.

Falsifier: a raw reason starts terminal work, a second outcome overwrites the
first, or `hard_terminal` constructs its own token.

### Cancellation

- Join tasks enter root custody before the first await.
- Cancelling an observer drops only observation; Authority remains recoverable
  and visibly nonzero in the root.

Falsifier: cancellation drops a join handle, actor, FIFO, session, terminal
transaction, or reports zero while owned work remains.

### Shutdown

- One root-owned no-ACK settlement handles the same owned domains as fatal and
  cancellation.
- Actor, FIFO, replay, queued and stored bytes, ACK, timer, endpoint, controller,
  and session ownership all settle before whole-root exact zero and `Joined`.

Falsifier: shutdown uses the legacy initial-A path in parallel, emits synthetic
delivery/application ACK, discards a join result, or publishes zero from the
controller/output subroot alone.

### Unknown unwind

- Unknown disposition remains sticky nonzero quarantine or exact known
  inventory.

Falsifier: unwind restores `Live`/`CurrentOnly`, mints another outcome, or
normalizes uncertainty to clean zero.

## First future code slice: CurrentOnly constructor tracer

This amendment designs but does not authorize the first Rust slice.

### Allowed future files and symbols

- `owner.rs`: add only private `mod physical_session_root;` plus the minimum
  net-zero or shrinking mechanical routing. Its exact grandfathered line count
  must not exceed `2062`.
- new `owner/physical_session_root.rs`: production deep Module, at most `1500`
  lines, owning the root state and constructor transaction.
- one private `owner/physical_session_root/tests/mod.rs` index, at most `300`
  lines.
- one private `owner/physical_session_root/tests/current_only.rs` leaf, at most
  `800` lines.

The production Module must document what it owns, what it does not own, its
invariants, entry symbols, and test location. No grandfather entry may be added.
If exact fixture reuse proves that initial-A Authority must first move, that
movement is a separate behavior-preserving slice with its own provenance and
review; it cannot be mixed into the constructor behavior.

### First RED

The first focused tracer is named exactly:

```text
settled_initial_seed_enters_current_only_root_with_exact_shutdown_authority
```

Its single expected causal failure is the absence of the complete private
production root-plus-constructor seam:

```text
owner::physical_session_root
OwnerPhysicalSessionRoot<T>
OwnerPhysicalSessionRoot::from_initial_seed
```

The compiler may first report the missing private Module, type, or associated
function depending on the tracer scaffold. All three are absent in the current
tree. The RED must not be preceded by a behaviorful production scaffold merely
to make the method appear to be the only missing item. Any unrelated compile
failure or proof that exact seed rollback needs a wider Interface stops the
slice before repair.

### Minimum GREEN

The minimum implementation, in the same single tracer, adds the private Module
route, root type, and constructor and then:

1. consumes exactly one settled `QuinnOwnerInitialPhysicalRootSeed<T>`;
2. constructs one closed CurrentOnly root;
3. records restart credit as `NotInstalled`;
4. exact-returns the seed on validation/pressure or output-root `Err` before the
   fused success edge;
5. inventories actor, handoff, actor-contained FIFO, controller/output and its
   Target executor, and structural shutdown/join custody; and
6. makes structural current-A shutdown root-owned without enabling production
   hard-terminal behavior.

It must not connect terminal behavior, B topology, runtime credit, public TUN,
or any switch route. It does not yet prove completed uniqueness because the
existing alternative exits and initial-A Authority still require an explicit
absorption/caller-cutover slice.

### Slice acceptance and limits

- Success proves one seed-to-root move and exact inventory.
- Rejection and pressure prove bit-identical exact return before publication.
- Cancellation proves dropping a construction observer cannot lose a consumed
  Authority.
- Structural shutdown proves current-A actor/handoff/FIFO/join custody is
  root-owned; it is not the later terminal finalizer.
- Fatal production entry, root-minted terminal capability, authenticated B,
  runtime credit, and whole-root joined zero remain explicit non-goals.
- Unknown postcommit disposition must stay visibly nonzero; no test may use
  `Drop` or an ignored join result as cleanup evidence.

Focused RED/GREEN, exact/repeated constructor and shutdown characterization,
owner-initial/controller-output regressions, Knife16 and repository provenance,
standalone/Cargo governance, format/diff/check, and independent review are
required before promotion. Any shrink-only ratchet is reported by the Writer
and changed only by the Integrator.

## Frozen later sequence

No item below is automatically authorized by this amendment:

1. this architecture amendment;
2. if exact reuse requires it, a pure initial-A fixture/Authority Move;
3. CurrentOnly production root construction;
4. absorption and caller cutover needed to make that root the unique production
   first-outcome/shutdown/join Authority;
5. root-minted committed outcome connected to production terminal
   preservation;
6. CurrentOnly root-owned no-ACK finalizer and whole-root exact zero;
7. authenticated B topology while the old B-only adapter stays test-only;
8. runtime restart credit only after controller progress and hint issuer share
   the same root Authority;
9. physical attach/lifecycle, owner outstanding/terminal/application-ACK
   observations, B probe/hint evidence, and the client single arbiter;
10. the no-oracle byte harness over success, rejection, pressure, fatal,
    cancellation, shutdown, and unknown-unwind quarantine; and
11. only after all physical R7a-d gates close, discussion of public TUN or
    external acceptance.

## Forbidden bypasses

- Ungating, importing, wrapping, or citing `owner/physical_root.rs` as
  production reachability.
- A free-standing outcome token factory or raw terminal-reason constructor.
- A second initial-A terminal, shutdown, join, FIFO, or controller owner.
- Numeric zero in place of `NotInstalled`, or a counter in place of a credit
  lease.
- Production-callable policy-only commit, scalar-equal acceptance, or one-way
  marker/slot validation.
- Re-export, pass-through method, `pub(super)` diffusion, callback, parameter,
  dynamic-dispatch, global-lock, per-record-allocation, macro, or `include!`
  workarounds.
- Acknowledgement publication before final infallible commit, or synthetic
  `DELIVERED`/application ACK during terminal settlement.
- Whole-root zero derived from a controller/output subroot, actor-only join, or
  ignored cleanup result.

## Writer boundary and stop rule

This status-promotion repair reaches its Writer coherent boundary only when the
accepted amendment and compact current documents agree that:

- hard-terminal extraction A-D is complete;
- CurrentOnly root Authority precedes production terminal behavior;
- root-owned no-ACK finalization follows terminal preservation;
- the test-only B adapter is not production evidence;
- static capacity remains necessary-only and runtime credit is uninstalled;
- R7a-d, Task 4, public TUN, and external acceptance remain open; and
- no Rust, test, governance, archive, or historical architecture file changed.

Independent architecture acceptance is closed. This canonical-status promotion
still requires its own document closure review, and the first CurrentOnly Rust
slice remains a separately scoped next gate. The Writer starts neither Rust nor
any later sequence from this boundary.

For every future code slice, stop immediately on an unexpected RED, a required
write-scope expansion, a wider Authority Interface, a second lifecycle owner,
capacity arithmetic without a deterministic witness, an oversized-file
increase, or a settlement model that cannot close success, rejection, pressure,
fatal, cancellation, shutdown, and unknown unwind at one root. Report the exact
evidence and minimum revised plan before any repair.

## Source-of-truth links

- `docs/current/STATUS.md`
- `docs/tech/2026-09-02-knife16-r7-hard-terminal-extraction-local-results.md`
- `docs/tech/2026-09-02-knife16-r7-hard-terminal-extraction-architecture-amendment.md`
- `docs/tech/2026-09-01-knife16-r7-physical-root-move-and-architecture-convergence-local-results.md`
- `docs/tech/2026-08-26-knife16-r7-no-oracle-switch-architecture-and-plan.md`
- `src/owned_upstream/quinn_leg_composition/owner_initial.rs`
- `src/owned_upstream/quinn_leg_composition/owner.rs`
- `src/owned_upstream/quinn_leg_composition/owner/continuation.rs`
- `src/owned_upstream/quinn_leg_composition/owner/continuation/hard_terminal.rs`
- `src/owned_upstream/quinn_leg_composition/owner/physical_root.rs`
- `src/owned_upstream/owner_session_controller.rs`
- `src/owned_upstream/reverse_switch.rs`

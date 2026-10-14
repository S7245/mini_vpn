# P3 — actual Owner source-service selection and readiness obligations

Date: 2026-09-07.

Status: **PLACEMENT AND OBLIGATIONS ACCEPTED; NO RUST WRITE SCOPE**.

This records a placement decision and the implementation-blocking obligations
found by source audit. It is not a complete binding Interface, a Rust plan,
or evidence that persistent service works. Full P3/Case C/R7/Task4 remain open.

## 1. Authority and precise claim

The full-P3 goal permits autonomous causal repair within its accepted scope.
The [scheduler amendment](2026-09-04-knife16-r7-target-open-ready-scheduler-amendment.md)
sections2/9–12/17–18 remain controlling. Same independent Reviewer Heisenberg
confirmed that doc-only refinement of the existing inward TargetIo readiness
contract belongs to P3; it does not itself open P6 or authorize Rust changes.
Missing local readiness is an implementation stop, not permission to busy-poll.

The [implementation plan](2026-09-04-knife16-r7-case-c-owner-server-root-implementation-plan.md)
still reserves actual table/bootstrap, fused adoption and whole-root caller
integration for P4/P5/P6. P3 may exercise the actual production-shared
Controller/Executor/output chain through a private fixture. A test resident
holding an arbitrary Driver is not production association. No public cutover,
Target worker/task/provider, external run, P3 commit or push is authorized here.

The [accepted read Move](2026-09-07-knife16-r7-p3-owner-source-move-plan.md)
is preserved exactly. Its tests and code are accepted, not rerun in this
doc-only stage. Exact evidence remains in the
[P3 result](2026-09-07-knife16-r7-p3-owner-progress-local-results.md).

## 2. Grounded missing seams

| Actual owner / source | Fact that the design must preserve |
|---|---|
| [Executor](../../src/owned_upstream/owner_target.rs), TargetReadSource and OwnerTargetExecutor | Owns the actual Flow, Driver, Target, Supervisor, effects and retained completions. There is no automatic ready-Flow selector. |
| [Read transaction](../../src/owned_upstream/owner_target/read_source.rs), try_read_target_observed | Reserves before Target read; then directly mixed-dequeues DATA/CLOSE and performs observed reduction. Splitting out only the dequeue loses the source-fact/failure transaction. |
| Executor apply_target_failure | Read/write/half-close failure enqueues RESET, mixed-dequeues it and submits its exact failure completion. This second Owner intake must join the same cutover. |
| [Checked Controller](../../src/owned_upstream/owner_session_controller.rs), new_checked | Derives one witness from the actual Executor and returns clock/executor/plan on modeled rejection. Local replay timing is sampled here; actor ACKs require the original same-clock arrival witness. |
| [Output root](../../src/owned_upstream/quinn_leg_composition/owner/continuation.rs), inbound transaction | Owns output preflight, entered-turn quarantine, page admission/rejection and continuation permits. Present ordinary input is actor-only; a local source turn is not already connected. |
| [Mailbox](../../src/owned_upstream/session.rs), bounded | Creates one ingress/inbox pair and shared progress; it does not bind a SessionId or Controller. Observer exclusivity alone cannot prove the pairing. |
| [TargetIo](../../src/owned_upstream/target.rs), readiness | A synchronous status snapshot with no Context/Waker registration. False readiness cannot justify a wake-driven park. |
| [TCP port](../../src/owned_upstream/tcp.rs), reserve_uplink_source / FactoryProgress | Reading also waits on exact queue, flow/session byte and segment capacity. Factory progress is not socket readiness or a reserved credit. |

The existing pending-completion selector also searches the Flow map. The new
hot route must use persistent bounded selection for these obligations too;
reusing a full-map search under a new name does not meet the amendment.

## 3. Design comparison and selection

Three read-only Interface explorations were performed under the architecture
skill; they are design inputs, not independent approvals:

1. **Executor-local intake**: place sender, inbox, two cells and local selection
   with the actual source. Best command/reducer Locality; changing the existing
   early bootstrap constructor complicates exact failure return after the
   one-shot source-factory mint, and does not alone close clock/output admission.
2. **Controller cold-pairing**: the checked constructor prepares one pair,
   installs sender/two-cell custody inward and retains its exact inbox. Best
   Leverage for the common checked caller; needs a narrow installation operation
   and explicit new capacity inputs, not caller-supplied unrelated endpoints.
3. **Whole output-root turn**: the outer entry hides source selection, observed
   reduction and output settlement. Best caller Depth; it must use the existing
   transaction, not introduce a second failure/continuation wrapper or reach
   directly into a Driver past the Controller/Executor.

Selected direction combines 2's cold construction with 3's whole-turn entry;
the actual two-cell Implementation and local Flow selection remain under the
Executor. This selects Authority placement, not unreviewed method signatures.

```text
whole physical root                         remains the eventual session owner
  existing controller/output root           output guard, failure, continuation
    checked Controller                      clock, exact inbox/prefetch
      actual OwnerTargetExecutor            same Supervisor/Target/Flows
        moved SessionCommands               one ingress/observer, exactly 2 cells
        bounded local readiness             non-owning indexes/cursors
        per-Flow actual source              same Flow + exclusive Driver
```

Process classes/budgets/admission/park stay in the outer scheduler. Inward code
must not import server_root or its scheduler types. Scheduling selection may
cross inward only as a finite value on the existing owning operation; no raw
Flow/Driver getter, re-export, callback or separately owned polling handle.

Policy and mailbox are in-process dependencies. MemoryTarget is a
local-substitutable Adapter; the later real TCP Adapter uses the same existing
TargetIo Seam. No second provider is justified. Deleting the deepened owning
operation would expose pairing, retained commands, timestamps and output
settlement to its callers: it must hide that work, not merely forward calls.

## 4. Cold pairing and publication requirements

Cold preparation consumes the original Executor, clock and typed capacity
inputs within the checked Controller construction transaction. It must:

1. Validate the actual factory origin, role, installed replay/Flow geometry,
   output capacity and cold/exclusive admission state before mutating inputs.
2. Derive the complete mailbox/local-index/storage envelope. M_ctl/M_data are
   independent positive typed inputs; existing source-channel limits cannot be
   relabeled as mailbox limits. The current capacity plan lacks these inputs.
3. Prepare one fresh pair, sole observer, persistent registration and all
   fallible storage before any installation. No Driver is polled and no Target
   operation, Waker publication or process registration occurs during this step.
4. Commit sender plus the existing complete two-cell Implementation into the
   Executor and the matching inbox into the Controller by infallible moves.
   Publish the checked Controller last. Do not expose an intermediate installed
   Executor or accept an independently prepared inbox.
5. Return every original input unchanged on modeled precommit failure. Empty,
   unpublished preparation storage can be released; existing ownership cannot.

Source-factory mint has already happened in the input Executor. A rejected
Controller build returns that same Executor, not a fictitious pristine
bootstrap that could mint the factory again. No fallible step may be appended
after mint while still claiming the earlier bootstrap rollback contract.

All existing entry/caller shapes and their migration must be frozen before
Rust: the current three-argument new_checked plan does not magically contain
the new bounds. Neither guessed defaults, unchecked compatibility controllers,
nor an externally callable late-install option are approved by this decision.
The symbol freeze must choose a complete checked construction shape, preserve
its exact failure payloads and reject duplicate installation before mutation.

Existing std/channel/Arc allocations are not fallible-OOM recovery evidence.
The future physical Layout and cold-peak proof must distinguish modeled
capacity/allocation rejection from process abort; no promise of recovery from
allocator abort is made. No fallible user callback is allowed after commit.

## 5. Exclusive source and observed settlement requirements

Move, do not duplicate, SessionCommands' cells, transfer phases, capacity
observer, rearm and cursor. There is one Control and one Ordered cell per
session, never a pair per Flow or another pair in the outer resident.
The move must include its narrow value dependencies without reverse imports.

For the bound Owner route, all actual Driver intake uses the lane-specific
ports. Both direct mixed-dequeue sites in section2 must be unreachable before
claiming exclusive service. Client sealed mixed receive remains a separate
route. Compatibility callers may not bypass the bound route by selecting an
old method; any temporary migration state needs a distinct audited contract.

Each pipeline phase keeps the same command/permits once: source queue, send
cell, mailbox/prefetch, reducer-held, effects or completion. An unknown send
retains unknown disposition, not a fabricated duplicate command. A failure
intent is retained before RESET staging and survives read/write/half-close
origin, Full, terminal-capacity rejection and delayed completion without a
second Target operation. A value-only Flow ID is not that ownership.

The existing output guard preflights before receipt consumption. The receipt
enters owned current work before any user-code/unwind point. Its exact observed
reduction, bounded effect page, observations, error and output admission settle
under the same Controller/output transaction. Raw apply_command is not a
substitute for the observed gate. No raw receipt escapes for a later caller to
reduce, and no second output failure or continuation Authority is introduced.

Local replay timing is captured at actual delayed reducer admission, not at
Target production or enqueue. Actor events retain their actual same-clock
arrival across delay/rejection; a naked mailbox Event cannot manufacture it.
A retained output page/continuation excludes another reducer admission until
the existing drain/permit protocol allows it. Source-only units may retain
bounded work but cannot consume or bypass that exclusion.

## 6. Readiness refinement: two distinct prerequisites

**Target readiness.** Refine the existing static TargetIo Seam to register and
recheck a named Flow's requested read/write interest using Context/Poll/Waker.
No socket/DNS/Tokio type crosses inward. Pending must mean a retained valid
registration can publish a later readiness edge; dropping the borrowed poll
must not erase it. Ready permits one later bounded attempt, not guaranteed I/O
success. WouldBlock re-arms under budget. Read and write waits cannot overwrite
one another, and unused writable readiness cannot spin a read-only source.
The old synchronous snapshot is diagnostic only, not a default implementation
of the new guarantee. MemoryTarget must prove the same lifecycle as TCP.

**Source credit readiness.** A readable Target is ineligible without source
queue space and exact flow/session byte and segment permits. ACK, terminal
release, rollback and queue dequeue have different capacity consequences.
The real TCP factory progress sequence exists, but a temporary borrowed async
wait is not proof that a root-owned poll registration survives observer drop.
Failed reservations themselves can publish rollback progress. Treating every
such notification as new available credit can create a self-waking retry loop.
Its current saturating epoch also does not prove the amendment's checked
exhaustion/fail-closed requirement.

The missing edge is concrete: Owner ordered dequeue returns a source FIFO
slot without FactoryProgress publication while the exact DATA byte/segment
permits can remain in downstream custody. Waiting for a future ACK cannot
prove that slot-only pressure wakes. Conversely, the rollback guard is created
before acquisition, so even failures that acquire nothing may publish. An
aggregate epoch has no Flow/reason information: replacing the baseline after
a failed probe can absorb another Flow's real release. Neither that shortcut
nor a bounded blocked-Flow sweep alone closes missing edges or self-retry.
Wake lock scope is also unproved: RESET progress publication currently occurs
under the admission guard. Metadata-unlocked notification evidence from the
mailbox cannot be transferred to this different primitive. The next contract
must address exact notification ordering and panic/re-entry with those locks.

The later [Owner-local credit contract](2026-09-07-knife16-r7-p3-owner-credit-rearm-contract.md)
now narrowly supersedes the retained-credit-registration mechanism in this
paragraph. Under its proved exclusive Owner/release-only prerequisites, local
source FIFO and permit releases directly request bounded rearm; no external
credit observer is added and generic TCP progress/Client rollback semantics
stay unchanged. This does not erase the audit findings above: any legitimate
out-of-turn release falsifies the choice. Target socket registration remains
mandatory, as do exact identity, no-self-retry, shutdown and fault obligations.
The protocol is independently accepted0/0/0, not implemented. No provider/task,
per-record waiter or full-Flow hot scan is authorized by this amendment.

## 7. Local selection and service accounting constraints

The single F-position Target/Flow index must preserve stable subslot and
generation; non-owning read/write/Driver/completion interest metadata and
summary words are separately byte-accounted. F includes opening/live positions;
O is a sublimit and Z only terminal SessionFlowId tombstones. Duplicated Wakers
do not mint a second lease. Old registrations/backing remain counted until
retired; reuse cannot make an old Waker target the new occupant.

Persistent cursors select a ready Flow/subsource; no drain-until-empty or
full-map hot search. Credit-blocked sets, pending completions, local revision
and clear/recheck must be included, not just readable sockets. Aggregate clear
requires every relevant local summary to be empty and stable under the
amendment's publication ordering. A borrowed polling future is not the owner.

Only these existing five Session sources may be used:

| Source | Required charged work; exact sub-selection still needs freeze |
|---|---|
| ControlSend / OrderedSend | One selected Driver lane pull OR one retained transfer OR one registration/rearm unit. Pull and send are not fused as two uncharged actions. |
| ControlInbox / OrderedInbox | At most one exact receipt through guarded observed reduction plus its bounded effect page. Recheck only into the existing reserved prefetch. |
| OwnerContinuation | Existing permitted continuation plus local Target production/credit readiness work through a persistent, bounded sub-selection. No hidden sixth source. |

The four dedicated process slices and B_session=1 stay unchanged. A positive
K denotes the eventual exact number of competing local subcategories, not a
new arbitrary config/default. With continuously eligible work, a conservative
candidate per-Flow delay has factors 5, K, F and T_cell(Session). The concrete
K, eligibility exclusions, number of pipeline visits and checked product must
be derived from the final operations before claiming a bound. The existing
5*T_cell source formula alone proves no per-Flow or end-to-end bound. Credit,
output drain or network waiting time is not continuous eligibility.

Likewise C_send=2N, C_prefetch=2N, C_mailbox=N*(M_ctl+M_data),
C_target_ready=N*F, C_source_ready=N*15+L_root and C_ready=4N+1 are constraints,
not current physical-capacity proof. All metadata/backing, independent actual
bytes and temporary cold coexistence still need checked Layout/TOTALBYTE.

## 8. Failure tree and required future evidence

| Branch / prediction | Required falsifier before service acceptance |
|---|---|
| Cold rejection or pressure | Same original Executor/clock/plan returned; no Driver pull, Target I/O, published registration or duplicate pair. |
| Success | Repeated real DATA then CLOSE/RESET through the same Driver, two cells, inbox, observed reducer and output root; exact replay permit retirement. |
| Full / Closed | Exact command returned/retained with no source repoll, invented zero or ACK; a later capacity edge permits only the specified retry. |
| Source-credit pressure | A failed reservation alone cannot sustain scheduling; actual relevant release wakes the right bounded candidate set after root Pending. |
| Delayed output / completion | Held receipt/completion and existing output permit exclude duplicate reduction/I/O; local timestamp and actor arrival remain distinct. |
| Observer cancellation | Drop/re-entry preserves actual command and all required readiness/credit registrations; no fresh ownership or first-registration shortcut. |
| Stale wake / clear race / exhaustion | Reused subslot cannot be scheduled by old generation; dirty publication survives clear/park; exhausted metadata fails closed. |
| Fault / unknown unwind | Same exact owning subtype remains sticky and nonzero; source-local failure does not disable unrelated session service. |
| Shutdown | Stop admission first; enumerate actual source/Driver, both send cells, mailbox/prefetch, reducer/effects/completion, output, registrations and leases. Missing settlement stays nonzero; never drain-and-drop or call closed zero. |

The shutdown work must retain the same whole-root custody for the later
accepted class migration; this selection does not implement parent destruction
or final no-ACK zero. A structural hash or constructor-only test cannot close
any runtime branch in this table.

## 9. Ordered next gates; implementation remains blocked

1. Preserve the accepted conditional Owner-local credit protocol and prove
   its complete release-entry/Drop coverage in the future bound route. Freeze
   the still-missing real Target socket registration contract; zero external
   credit observers cannot replace it. The generic factory audit is neither
   repaired nor used as readiness proof by the selected local mechanism.
2. Complete the coupled construction and quantum freeze: concrete typed input
   migration, exact return/commit shape, local categories/K and service math,
   full physical capacity, inward visibility and the existing output guard.
   Re-review these contracts; placement approval does not approve them.
3. Symbol-first freeze each reversible Move and then one causal RED/GREEN
   tracer through the real owning chain. No Rust file is authorized here.
   Oversized Executor/Controller/Target sources cannot grow; feature Writer
   never edits the debt inventory. A coherent Move precedes behavior change.
4. Run the frozen local regressions/provenance/fmt/diff/check/governance and
   the same independent Reviewer for each coherent implementation. Do not
   claim P3 until its complete result ledger is satisfied, including remaining
   process classes and static Target-open/MemoryTarget/TCP/DNS work.

Architecture hard stops still apply to new providers/public exports/dyn/tasks,
reverse dependencies, extra cells/sources, legacy intake bypass, unbounded
polling, publication before fallible work or unsupported zero. Causal in-scope
repair authority does not waive these. No agent may implement around an open
contract by choosing a convenient local default.

## 10. Review and evidence

Main is the sole Writer; Linnaeus, Descartes and Leibniz supplied distinct
read-only Interface designs. Mencius audited the actual constructors and TCP
credit path. Heisenberg remains independent of the design and reviews this
decision and its honest implementation stop, not an imaginary completed port.

Pre-stage verification matched all31 accepted fingerprints. A separate
immutable entry covers1143 repository files including336 Rust sources:
`/tmp/mini-vpn-p3-source-service-v3Owhx/entry.json`. This stage permits only this
decision, the compact STATUS/HANDOFF/TODO, P3 result and local LEARNINGS.
No Rust, tests, governance inventory, prior accepted architecture or ERRORS
write is allowed. Doc checks and unchanged-source fingerprints are required;
no Cargo/test evidence is manufactured for a doc-only decision.

Independent scoped assessment: Module/Clean/System/DDIA=8/8/6/7.
To reach10/10 for implementation readiness, close sections6–9 with concrete
representations, source-derived service/storage bounds, deterministic owning
tracers and independent approval. These are design scores, not a percentage
of P3 completion. First independent review: P0/P1/P2=0/0/1, Request Changes
only for the missing explicit FIFO/aggregate/lock facts in section6/next gate.
Those facts and proof obligations are now added without selecting an algorithm
or Interface. Same Heisenberg re-review: P0/P1/P2=0/0/0, Approve, limited to
placement and remaining obligations. All six documents agree;336 Rust sources
are unchanged,26 local links resolve and no out-of-scope mutation was found.
`repair-review.json` binds the reviewed documents; `final.json` separately
records the factual promotion, which Reviewer explicitly allowed without
another review. Full binding, Rust scope and full P3 remain unaccepted.

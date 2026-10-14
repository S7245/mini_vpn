# P3 — closed Owner source-credit rearm contract

Date: 2026-09-07.

Status: **CONDITIONAL PROTOCOL AND DOCUMENT CLOSURE ACCEPTED; NO RUST WRITE SCOPE**.

## 1. Decision and explicit amendment

For the closed actual Owner route, use Owner-local release-driven rearm.
Do not subscribe this route to the generic TCP FactoryProgress epoch and do
not add an external credit observer. This is a conditional mechanism choice,
not proof that the future binding already preserves its prerequisites.

This narrowly replaces the retained-credit-registration mechanism required by
section6 of the [source-service decision](2026-09-07-knife16-r7-p3-owner-source-service-decision.md).
It does not replace any missing-edge, no-self-retry, cancellation, failure,
capacity or custody obligation. Independent contract review confirmed that
this doc-only alternative can be considered within full P3; that review did
not approve the ownership proof or algorithm below.

The [scheduler amendment](2026-09-04-knife16-r7-target-open-ready-scheduler-amendment.md)
requires real work to republish local bit → summary → aggregate → root epoch;
it does not prescribe a FactoryProgress observer. Target **socket** readiness
still requires a real retained registration through existing TargetIo.
Zero external credit registrations is not zero Target/Driver/actor/mailbox
registrations, and not zero source-credit ownership.

Public/Client TCP reservation, rollback publication, wait_for_progress and
shared-ledger semantics remain unchanged. No Rust Interface, provider,
getter, callback, dynamic future, task or public route is introduced here.
P4–P7, external acceptance, P3 commit/push and full-P3 completion remain closed.

## 2. Source-grounded exclusivity proof and its limit

The current actual Owner path has these properties:

- [TargetReadSource / Executor](../../src/owned_upstream/owner_target.rs)
  privately retain both source endpoints, the exact factory, Target and
  Supervisor. OPEN installs both results of the same factory open_flow.
  There is no factory/Flow/Driver extraction or cloned factory return port.
- [try_read_target_observed](../../src/owned_upstream/owner_target/read_source.rs)
  is the only production reservation caller for those Owner sources. Its
  reservation remains in a synchronous mutable Executor borrow; it is neither
  split, spawned nor returned. Failure RESET uses the same private source.
- [TargetIo](../../src/owned_upstream/target.rs) receives Flow ID/bytes and
  Target operations, never these source endpoints, factory or source permits.
- [Supervisor](../../src/owned_upstream/supervisor.rs) retains actual
  UplinkReplayOwnership in private uplink_replay/quarantined_uplink storage.
  Complete-extent reducer ACK or exact FlowFinished releases it; partial ACK
  retains the complete permit. Source DATA/rejections can retain it earlier.
- OwnerTargetOutput carries frames/lifecycle/resume values, not those source
  permits. Transport queue/Frame Drop is not a source-credit release route.
  Bytes backing lifetime is distinct from permit lifetime.

The future moved two cells and paired inbox must remain below the same whole
Controller/output owner. No receipt carrying private Owner DATA may escape as
a caller-owned normal continuation. Output/actor/Target adapters cannot gain
a same-factory producer or releaser. Source operations remain synchronous
within one owning turn; only borrowed observation may be cancelled.

This is not a proof about public ResumableTcpFlow in general. The Client root
returns a Flow, TUN splits it, and other tasks retain related capabilities.
Distinct flows have distinct admission locks and shared session semaphores.
Suppressing a generic rollback can therefore lose a real other-flow release:
A takes the last shared byte, fails later on its own segment limit, while B
observes the temporary exhaustion; A's rollback must still wake B.

**Falsification rule:** any normal recoverable same-factory release outside
the admitted Owner turn disproves zero-registration. Stop binding and revise
this contract; never label that legitimate release fatal merely to keep this
choice. Whole-owner transfer preserves exclusivity, but parent destruction
and final no-ACK settlement remain separate unclosed work.

## 3. Why Owner-local rearm is sufficient under that proof

During an Owner source-production quantum, no second same-factory producer
can inspect or acquire the temporarily reserved capacity. A failed reservation
or WouldBlock restores its own speculative resources before the turn ends.
Successful DATA consumes credits; short-read shrink only returns the unused
part of the same speculation. None makes another previously blocked source
more eligible than it was at this quantum's entry.

Accordingly those operations do not request a local credit retry. Their
existing generic FactoryProgress publications remain untouched and unobserved
by this Owner scheduler. This suppresses a **local reaction**, not the shared
publication needed by Client/public consumers. No post-failure cursor reset
is used, so there is no mixed-epoch baseline to swallow another release.

Every normal event that can increase Owner source eligibility is instead a
known operation in the same owning chain: an actual source FIFO dequeue or an
actual source-permit release during reducer/effect/cleanup service. The
following sections require those operations to retain scheduling work before
returning or yielding. No new thread, waiter or global source scan is needed.

The source-production and release-only phases must be genuinely separated in
the final route. Both current mixed dequeues are cut over together: DATA/EOF
production and failure RESET stage into the actual Driver/mailbox path; they
cannot perform a hidden reducer/terminal release inside the production phase.
The phase split is an implementation prerequisite, not an assertion about the
current combined read function.

## 4. Exact retryable states and separate gates

The local credit state distinguishes a capacity-blocked source from a source
with one retry requested. These are mutually exclusive owning-phase facts,
not unrelated booleans that can disagree with ready membership. They own no
permit; the physical permit owners remain unchanged.

Retryable pressure is limited to source message-slot, flow/session byte and
flow/session replay-segment exhaustion with an already validated request.
Closed, sealed admission, invalid length, offset exhaustion, wrong origin or
unexpected active reservation are not generic Pending credit. They follow
their exact existing terminal/invariant disposition. A reservation may not
remain active across a service yield in this contract.

Socket readability and source credit are independent gates. A Target Waker
can mark socket readiness but cannot clear credit-blocked state or repeatedly
retry reservation. Only a corresponding local rearm or initial unblocked
source service grants that attempt. If it then encounters pressure again, it
returns to blocked once; no self-request is generated. If Target returns
WouldBlock, its real readiness registration owns the next socket wake.

Output admission, continuation permits, completion/Z pressure and credit are
also distinct. This credit index cannot retry a retained failure completion
or bypass an output page merely because bytes became available.

## 5. Source FIFO release: direct, flow-local rearm

A selected Ordered Driver pull has a known actual Flow and a vacant session
cell. The received command is first committed into that existing cell; only
then may the exact Flow's slot-blocked state be changed to retry-requested.
Publish its existing local readiness and aggregate before leaving the unit.
No second Driver poll, Target read or mailbox send is hidden in that unit.

DATA retains its byte/segment permits after dequeue. Slot-only release still
rearms when those remaining quantities are sufficient, without waiting for
an ACK or generic factory publication. Pending creates no local slot-release
claim. None/closed follows actual source closure, never session zero.

The receipt-before-publication ordering is mandatory. The existing Owner
source producer uses try-only reservation; the final symbol/primitive audit
must prove that source-slot return cannot invoke an awaited producer's Waker
before receipt custody. If that proof fails, this direct rearm cannot silently
proceed; use the architecture hard stop. Actual source-input release on an
error path must remain owned or faulted, not dropped then retried.

## 6. Permit release: constant-size observation under the owning turn

An admitted **release-only** reducer/effect/continuation/cleanup unit records
the exact factory's aggregate owned source bytes and segments at entry. The
existing factory operations already expose these aggregate observations; no
new raw semaphore/provider/getter is needed. The record is value-only
readiness evidence, never a substitute for any physical permit or zero audit.

In this phase no uplink source reservation is allowed. Binding a previously
reserved command into replay moves existing permits, not new acquisition.
ACK, terminal, rejected-work disposal and source retirement can only preserve
or decrease the two aggregate owned quantities. The implementation audit must
verify this classification for every normally recoverable entry, not just ACK.

Before either an Ok **or** a normally recoverable Err can return, compare the
two actual quantities again. A decrease in either requests one global local
credit sweep. No decrease requests none. An increase contradicts the
release-only classification and must not silently produce a clean/no-progress
result. Partial ACK that advances a protocol offset but keeps the entire
permit therefore does not masquerade as available source credit.

This observation is outside the individual inner release call, so it includes
cleanup later in its same bounded effect page. It uses two aggregate reads
before and after, not per-Flow/per-replay enumeration. Full existing Supervisor
or inventory complexity is not claimed constant-time by this optimization.
Operations that can both acquire new source permits and release old ones may
not use net-delta inference: they require the separated phases above or a
new reviewed exact release mechanism.

The observation interval is a phase of the existing whole Controller/output
entered-turn transaction, not a second lifecycle or failure Authority. Before
any release-capable call, that transaction retains enough phase information
to reject normal re-entry until the observation is settled. Finalization and
local rearm precede output/observation publication and any Pending yield.
No new callback-style wrapper or caller-side post-Ok rearm is acceptable.

On unwind, the same transaction retains the exact command/error/disposition
and enters the existing sticky nonzero quarantine. It must not return to
ordinary service with an unfinalized observation. This covers a release that
occurred before its inner call panicked; it does not invent a rolled-back
permit or pretend a failed session still satisfies live-service liveness.
Any normal resumable error must use the finalize-and-rearm path, not this
quarantine escape. This distinction requires separate deterministic tracers.

## 7. Bounded blocked-source sweep

Keep credit-blocked membership in the existing F-position Flow readiness
storage, with the actual Flow/subslot generation. It adds no owning Flow,
command or ready lease. A preallocated hierarchical summary selects a set bit
after a persistent cursor without reading every Flow/map entry. Its bounded
word/tree work and backing bytes belong in the final checked Layout.

A genuine aggregate release sets a local sweep request. No publisher walks
the blocked set. When the OwnerContinuation category services credit work:

1. If idle and requested, begin a circular pass from its persistent cursor.
2. Select at most one blocked position in the unvisited interval of this pass;
   validate its generation, change it to retry-requested and publish the
   existing source readiness. Advance before returning.
3. The actual Target/source attempt is a later separately charged unit, not
   part of the sweep. Pressure can put it back into blocked membership.
4. Requests arriving during a pass set one sticky dirty/request flag; they do
   not reset its cursor or repeatedly put earlier Flows first.
5. At pass end, start another pass only if requested since this pass started;
   otherwise settle idle. Do not clear a newer request while ending the pass.

Selection processes at most F candidates plus one end check per pass. A Flow
that becomes blocked after its interval was visited has already observed
capacity at that later time; a subsequent real release dirties the pass and
causes the required next pass. Source-slot release directly marks its Flow
and does not trigger an all-Flow sweep. Retired generations never inherit an
old blocked/retry state. Cold audit checks index membership bidirectionally.

Credit requests are mutated by the exclusive Owner, so their coalescing need
not use the generic factory epoch. Existing checked local/root ready revisions
still govern publication and fail closed on exhaustion. External Target
Wakers cannot clear credit requests. The owning pass/request state survives
borrowed-observer cancellation. Source work/aggregate clearing must see that
state; a pending pass forbids root park even if its next candidate is not yet
marked retry-ready.

Only OwnerContinuation services this bounded credit work. Four process classes,
five Session sources, B_session=1, F Target positions and two send cells stay
unchanged. K_cont is the later exact number of eligible competing continuation
subcategories; it is not an arbitrary new parameter or accepted default.
With continuously eligible service, a release during an old pass needs at
most 2*(F+1) credit units to revisit a previously visited blocked Flow. A later
source-production selection can add at most F such source-category visits.
Thus a conservative candidate release-to-attempt bound is:

```text
T_retry <= (3*F + 2) * K_cont * 5 * T_cell(Session)
```

All arithmetic must be checked with the eventual exact category mapping before
any bound is published. This formula is a composition obligation, not current
runtime evidence. Capacity may be consumed by another Flow before an attempt;
continuous eligibility excludes missing credit, output permission or socket
readiness. It is not a throughput or starvation-free credit-allocation claim.
Repeated real releases coalesce into active/dirty passes; they do not each
allocate or execute a whole F scan on a DATA hot path. One full blocked pass
can still cost O(F) total work spread over budgeted turns; do not call it O(1).

## 8. Fault, shutdown and scope conservation

No new credit observer means no additional credit Waker/Notify allocation,
task, lease or unregister protocol. Existing Target/Driver/mailbox readiness
and their publication fault model remain required. Generic FactoryProgress
lock/Wake/saturation facts are not repaired or reused as proof here; the Owner
route simply does not wait on that primitive. A later observer-based fallback
would need its own reviewed closure of all those facts.

Source-local unknown keeps the actual session subtype nonzero; scheduler
corruption/exhaustion follows the already accepted root-fatal Authority.
Readiness rearm cannot clear either fault. The production binding must make
all normally recovering releases pass through section6, not rely on a test
fixture's later explicit wake or manual ready-bit insertion.

Shutdown closes source admission first. Requests/pass state remain with the
whole Owner while existing scheduling migration selects only sources legal in
that retained phase. A credit release after shutdown cannot republish live
DATA production. Settlement retires blocked/retry membership only with the
actual source/Flow disposition; metadata emptiness is never physical zero.
Queued/source/cell/mailbox/reducer/effect/completion permits, independent bytes,
actor/output/endpoint/session ownership remain in their existing ledgers.
No drain-and-drop, seed reconstruction or synthesized no-ACK success is added.

## 9. Exact future falsifiers and next implementation gate

The tracer surface is the real bound Controller/Executor/output chain, using
MemoryTarget and actual source/reducer permits. Helpers cannot set readiness
after the event they are meant to prove. Add one causal tracer at a time:

| Behavior | Decisive observation |
|---|---|
| Failed reservation quiescence | With another Flow retaining global credit, repeated permitted Owner polls reach Pending; generic rollback epochs cannot keep this Owner runnable. |
| Source-slot-only release | Real Ordered dequeue requests another source attempt before ACK, while the first DATA still owns permits. |
| Global complete ACK | Exact reducer ACK frees one Flow's extent and automatically schedules another blocked Flow through a budgeted pass. |
| Partial ACK | Protocol progress without whole-permit release does not create credit availability or a retry loop. |
| Recoverable rejection/cleanup | Actual released credit is rearmed even when the owning turn returns a modeled recoverable error. |
| Release then unwind | Caught interruption cannot resume ordinary service or lose the exact retained phase into false zero. |
| Borrowed observer cancellation | Active/dirty pass and actual permits survive; re-entry services the same pending work without manual wake. |
| Release during pass / reuse | A later release cannot be cleared at pass end; stale Flow generation cannot schedule its replacement. |
| Shutdown | A late release produces only allowed settlement progress, never new live DATA or invented whole-root zero. |

Before these tests can authorize binding implementation, complete the coupled
freeze of actual cold pairing, Target socket registration, concrete categories/
K_cont, physical Layout/cold peak, release-only entry coverage and exact
guard/return representation. Neither new Root resident wrappers nor a toy
counter test satisfy that freeze. No Rust allowed-files are opened here.
Oversized-source Move/provenance and Integrator-only ratchets still precede
behavior changes. Full P3 includes the remaining process/Target-open/TCP/DNS
ledger; this credit contract cannot redefine that objective.

## 10. Evidence and review status

This stage is source-level design, not a reproduced runtime credit bug or a
passing implementation. Diagnose/TDD are at their plan/feedback-loop gate;
the architecture stop precludes manufacturing a test against an unaccepted
binding. Source evidence changes the next action from new generic observer
design to a closed local owning-turn design.

Mencius independently traced current Owner ownership and public/Client
counterexamples without designing the protocol. Main owns this proposal;
Heisenberg remains the independent Reviewer. After his scope-only ruling,
full contract review returned P0/P1/P2=0/0/0, Approve, limited to the stated
exclusive Owner/phase prerequisites. No necessary optimization was found.
Mencius's separate follow-up found no new source acquisition in the current
effect/resume/recovery/retry/cleanup closure excluding Target-read production;
OPEN creates empty endpoints and DATA binding moves existing permits. He
also found no normal private Owner DATA escape in those current paths. The
combined old read function and the unimplemented future binding are explicitly
not covered by that release-only classification. No runtime test was run.

Entry revalidation matched the preceding six accepted documents and336 Rust
sources. The immutable1144-file snapshot is
`/tmp/mini-vpn-p3-owner-credit-VsDY1G/entry.json`. Allowed writes are this
contract, the explicit source-service-decision amendment, STATUS/HANDOFF/TODO,
P3 result and LEARNINGS. No Rust/tests/Cargo/governance/ERRORS/prior scheduler
amendment/commit/push/external changes occur in this doc-only stage.

Independent scoped Module/Clean/System/DDIA:8/9/7/8. Concrete representation,
complete entry coverage, checked storage/service composition and actual owning
tracers remain necessary for10/10 implementation readiness. `draft.json` and
`awaiting-review.json` bind the reviewed unchanged contract. Same Heisenberg
final seven-document closure: P0/P1/P2=0/0/0, Approve; sections1–9 inverse-match
the approved contract,336 Rust and HEAD/index are unchanged, and34 local links
resolve. `docs-review.json` binds that packet; `final.json` separately binds
the permitted factual promotion. Reviewer allowed that transcription without
another review. No binding/Rust/full-P3 acceptance follows from this verdict.

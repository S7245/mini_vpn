# P3 — closed command route, owning turns and service categories

Date: 2026-09-07.

Status: **SELECTION/PROTOCOL ACCEPTED; JOINT FREEZE OPEN; NO RUST GRANT**.

## 1. Scope and selected approach

This refines the accepted [Flow completion contract](2026-09-07-knife16-r7-p3-flow-completion-contract.md)
and [construction direction](2026-09-07-knife16-r7-p3-binding-construction-phase-decision.md).
The actual Controller/Executor/output Module remains the Authority. This
decision selects a closed bound call graph using the existing command carrier,
not a new boxed Owner wrapper or a second mailbox. It also assigns output and
recovery service that the preliminary eleven-category list omitted.

The full-P3 goal permits causal in-scope refinement, not bypassing independent
review or the joint cold/domain/Interface/Layout gate. No Rust, governance,
public route, P4–P7 implementation, external gate or commit/push is authorized
by this document. Real parent/physical caller wiring remains the later accepted
stage; a private production-shared P3 fixture does not prove that wiring exists.

## 2. Carrier proof: closed production graph, not a Driver label

Current [DriverInput](../../src/owned_upstream/tcp.rs) at1762 permits arbitrary
SessionEvent inside Control. The [Supervisor test](../../src/owned_upstream/supervisor.rs)
at3362 explicitly wraps peer OPEN as Driver and reduces it. The mailbox moves
that variant unchanged; it does not authenticate local origin. Current
CommandOrigin::Driver(SendLane) at [server_root](../../src/owned_upstream/quinn_leg_composition/owner/server_root.rs)
225 and its borrowed arbitrary Driver at325 are only conditional primitives.

Select the following closed route for bound production service:

1. The actual Executor selects one retained Flow/source by its persistent
   local index. Cold and per-turn checks correlate index generation, complete
   LocalFlow and the same source pair/factory. No caller supplies a Driver,
   SessionOwnerCommand or numerical FlowId as an alternative receipt source.
2. With a vacant preflighted cell, poll only that actual source's selected
   physical Driver lane. Install returned custody before validation, callback
   or yield, as the current primitive already does. A preselected Flow key
   enriches the cell's existing origin; it is not another command.
3. The only bound Driver classes are DATA, LocalClose and LocalReset with
   TargetFailure. Sink completion, PeerFrame, Open and ClientDriver variants
   are forbidden here. Unexpected actual input stays owned in the cell and
   quarantines the session; do not drop it while reporting a class error.
4. The same private ingress, two cells and capacity observer move inward as
   one SessionCommands Implementation. No clone/export of ingress, raw-send
   method or borrowed-Driver service remains reachable from a bound external
   caller. Open completion uses only Event(PeerOpenResolved) with its existing
   exact request custody; it never increments Driver transit accounting.
5. Controller receives only from its cold-paired inbox. At receipt and carrying
   rejection, validate the entire LocalFlow value (session, Flow, direction,
   authority_id), legal variant and current retained Flow phase. For DATA,
   preserve the real factory-origin/permit checks. FlowId or lane alone cannot
   settle transit or authorize terminal release.

This is a call-graph/encapsulation proof, not a claim the shared carrier is
unforgeable across all crate code. Shared Client/legacy tests keep their
existing semantics. The bound route's private pairing, legitimate producer
set and complete intake validation must be executable provenance/test gates.
If a raw ingress/receipt escape is required, this selection is falsified; do
not silently rely on the Driver tag or add an opaque wrapper as a local patch.

The bound Executor must not expose its old synchronous source/reducer bypass.
Its legacy read/RESET mixed-intake operations reject bound use before reading,
reserving or dequeuing; unbound pre-construction characterization remains
distinct. Binding cannot be cleared by a getter/extractor. Checked Controller
owns the bound Executor privately; the new owning service entry never returns
it. Actor input stays on the existing immutable-arrival observed route, not
through the local mailbox. No new permit re-export through owner.rs is selected.

## 3. Exact transit and rejection shape

Keep Driver transit as checked accounting in the real retained Flow state.
Successful actual Driver pull plus cell installation adds one. Moves to mailbox,
prefetch, current reduction or carrying completion do not add another. Physical
commands remain the evidence; the count is not authority and cannot erase an
unknown disposition. Preflight increment capacity before polling; an interrupted
pull remains covered by the existing entered source/visit quarantine.

The invariant in every known settled phase is the sum of matching actual
Driver-origin commands in those positions. While a turn is entered/unknown,
retain that exact phase and any conservative residual rather than asserting
this known-phase equality or manufacturing an absent command. No destructor
decrements transit merely because a wrapper went out of scope.

Mailbox Full/Closed preserves the same command Box in its cell. Supervisor
Control pressure rebuilds the same variant/event (1175/1241), preserving the
full LocalFlow but not the original Box address. The matching Flow retains a
Z-rejected RESET without restaging it. Retry uses that same owned command.

Do not generalize Control rejection to DATA. Supervisor1669 may move real
DATA staging into quarantine or return a non-carrying reducer error; the root
must preserve that actual outcome, not fabricate a new UplinkData. Bound late
DATA first uses the accepted terminal-disposition path. Unexpected DATA error
keeps the existing failure/quarantined staging evidence and nonzero session
disposition; it is not a normal receipt retry or successful transit settlement.

Cold already-carried completions enter accounting at their actual location.
A failed Open with no published source cannot accept Driver feedback. Failure
reconstruction at owner_target1934 must compare the complete LocalFlow, not its
current FlowId-only predicate. Wrong identity is detected before consuming
reduction or releasing original work.

## 4. Three modes of the existing owning turn

Use the existing root state, not a parallel lifecycle/failure ledger:

| Selected work | Existing owner / permitted mutation |
|---|---|
| Local inbox or completion/write reduction | Ordinary output admission; no actual continuation or pending output/lifecycle may be bypassed. |
| Actual effect continuation | Existing drained-page continuation transaction and its existing private permit. |
| Source/metadata/Target probe or already-authorized retirement | Deepened existing recovery_service transaction state, tagged by finite work kind; may coexist with an unrelated retained output page, but cannot reduce input or consume a continuation permit. |

The whole entry stays on QuinnOwnerControllerOutputRoot. Its private operation
selects the Controller work and handles the actual result before releasing the
guard. Inward Controller/Executor calls are complete owning operations, never
caller-visible begin/end callbacks or raw receipt extractors. No new separate
source permit/re-export or optional actor-slot callback is introduced. Exact
Module placement/visibility and the single production caller set still require
the joint symbol freeze; this is not permission to spread helper methods.

Ordinary inbox sequence is: preflight output/phase exclusion; poll the selected
inbox lane into Controller-owned receipt/prefetch; validate origin and timestamp
at actual admission; mark the existing guard entered; perform observed reduction
and its bounded effect page; retain observations/errors/outputs; settle output
admission last. The mailbox's own prefetch protects an unwind in capacity
notification before the receive returns. No owned receipt is held only in a
cancellable observer future.

Pending without a receipt restores the unentered vacant phase explicitly.
Expected carrying pressure retains one actual command at its legal current
position or matching Flow completion, then explicitly settles the empty-output
ordinary turn. It is neither a continuation wait for itself nor root failure.
Post-entry uncertainty uses existing quarantine; a normal recoverable Err must
not be normalized through that unknown branch. Actor errors/arrival handling
are not globally relaxed by this local-input distinction.

Source-only work never calls the old combined read/dequeue/reduce method.
Source production may reserve then read/commit DATA/CLOSE, or retain failure
work and stage RESET. Reduction occurs in a later separately charged inbox
turn. Write retry differs: positive acceptance reduces SinkAccepted and may
produce effects, so it requires ordinary admission rather than source-only
permission. Join/retirement uses retained terminal authorization, not a live
read/write grant. Cold/non-Active/recovery phases derive legal work explicitly;
none is made Active just to reuse a helper.

The source factory's actual aggregate owned bytes/segments are sampled inside
every release-only Executor operation. Finalize release-driven credit requests
on normal Ok and recoverable Err before returning, then finalize deferred outer
notification after inner admission guards are gone. Existing entered root state
contains unknown interruption. Source reservation/production is not mixed into
that release interval. No caller-side post-Ok hook, full snapshot per source
packet, new global observer or second failure Authority is selected.

## 5. Explicit service catalogue

The preliminary K=11 directory was incomplete: output drain/lifecycle precedes
continuation eligibility, and recovery has its own retained phases. Select the
following **thirteen** competing OwnerContinuation categories, not a tunable K:

| # | Category | One charged visit |
|---|---|---|
| 1 | Output/lifecycle | One existing output drive or one existing lifecycle settlement stage; never silently drain a whole page. |
| 2 | Effect continuation | One existing continuation page, at most E=max_effect_steps work items. |
| 3 | Recovery | One actual Acceptance/Execute/Publish/Handoff/terminal-disposition phase, preserving its current owning payload. |
| 4 | Target common notification | One candidate or pass-end check in the accepted bounded common pass. |
| 5 | Credit sweep | One blocked Flow candidate or pass-end check. |
| 6 | Read readiness | One named Flow registration/recheck; no hidden read. |
| 7 | Source read attempt | One reservation attempt and, if admitted, one Target read/commit or failure staging. |
| 8 | Write readiness | One named Flow registration/recheck; no hidden write. |
| 9 | Write retry | One pending write attempt, observed acceptance and at most one E-work page. |
| 10 | Join readiness | One named Flow registration/recheck; a readiness hint is not a Target tombstone reservation. |
| 11 | Join retry | One actual named Target join and any exact retire-after-join action. |
| 12 | Retained completion | One indexed exact completion retry plus its observed E-work page; no F-map search. |
| 13 | Publisher/record retirement | One retained Flow retirement stage or publisher-release check; not an arbitrary batch of Target operations. |

Each has a persistent circular cursor where multiple Flows compete. Fixed-size
category selection skips ineligible categories without invoking their I/O;
one visit executes only the selected category. Advancing before return prevents
a continuously republished early category/Flow from resetting another's place.
Credit/common passes retain their existing bounded pass and dirty semantics.

The five Session sources stay ControlSend, OrderedSend, ControlInbox,
OrderedInbox, OwnerContinuation. Driver pull and held transfer are separate
ControlSend/OrderedSend units; capacity observer registration/rearm is an
alternative send-source unit, as current server_root698 already charges it.
Terminal source drain also uses these physical lane units. Late command
disposition uses the inbox source, not an uncharged sweep. No sixth source or
separate per-Flow send cells are added.

Output/recovery phases require their actual existing physical queue/lifecycle
ports; their registration and owning caller cannot be replaced by a success
stub. When unavailable they are ineligible and retain custody, not endlessly
self-ready. Later physical caller wiring stays unimplemented here. The joint
binding freeze must name the real source of those ports and its bounded call
route before any executable service or end-to-end progress claim.

## 6. A category visit is not one I/O or constant CPU time

Current [Executor work loop](../../src/owned_upstream/owner_target.rs) at4361
already limits a page to E work items. One work may half-close, reset, cancel,
join and retire; ExpireNextTarget can perform three Target operations. Charge
these explicit contained operations, not only the outer method name. Write
retry can perform more writes in its following E page. Output admission/tail
and inventory checks can walk the E-output page as well.

Current recovery Publish at3973 traverses its retained page, stopping on
pressure. It is not one frame. Its actual page length, encoded bytes and ready
values must be bounded by the same checked page/recovery capacity in the joint
work/storage witness. No claim that max_effect_steps truncates this separate
loop. Its current source-derived recovery envelope is
`1 + Z + 3*F + S_ct + S_tc`, with checked arithmetic; S values are the actual
two directional session replay-segment capacities, not estimated constants.

The old grace-expiry path at2437 also calls the full pending-join scan at5202
and the completion scan at4975 **outside** E. On the bound route, exact grace
retirement must only settle that tombstone and request indexed join/completion
work for later charged visits. Bounded common/Z release visits provide actual
rearm; no unconditional retry loop or full-map fanout is hidden in a callback.

Existing reducer snapshot/terminal/replay paths themselves contain F/segment
walks: model snapshot1507, Supervisor1814/1897 and Controller3602. They are not
made O(1) by choosing K or observing fewer outputs. The final per-unit witness
must account for those configured finite populations and actual byte work;
any optimization/extraction is a separate causal behavior-preserving step.
Do not add more snapshots to the DATA source-production path. This stage
proves neither throughput nor wall-clock latency.

For the selected catalogue, K_cont is13 in the eventual checked composition:
an eligible category needs at most13 OwnerContinuation visits, and a named
eligible Flow at most F visits of its category. Four process budgets and the
five-source schedule remain as accepted. Thus the candidate scheduling-delay
composition is `13 * F * 5 * T_cell(Session)` for one continuously eligible
indexed Flow operation, not its entire protocol or I/O completion.

Preserve a Target attempt hint when source credit preflight fails: consume it
only when the actual Target operation is attempted. Then genuine credit release
does not also require an unaccounted fresh readiness probe. Under that explicit
hint/continuous-eligibility premise, the earlier credit contract composes as
`(3*F + 2) * 13 * 5 * T_cell(Session)`. Otherwise this bound does not apply;
required additional probe/notification visits must be counted. Every sum/product
must be checked at cold construction. This is the proposed catalogue/math,
not a passed runtime fairness test or complete work/Layout witness.
Actual ports or expiry settlement that change categories or their charging
require rechecking K and these formulas before executable catalogue acceptance.

## 7. Terminal and shutdown limits

Flow terminal uses the accepted proof and presence-sensitive retirement model.
Do not generalize it to whole-session expiry: reducer1693 clears the session
and emits SessionExpired, not a FlowFinished for every retained Flow. A bound
expiry route needs that actual session-terminal authorization for source and
late-command disposition, preserving full Flow correlation and emitting the
real session observation only once. No per-Flow terminal/tombstone may be
invented. Its concrete retained proof and class-migration caller remain part
of the joint lifecycle freeze; the old ExpireNextTarget remove is not a bypass.

Shutdown stops production/admission before rearming settlement-only work.
Unknown send/receipt/publisher custody and unavailable physical ports remain
nonzero. Neither the catalogue nor a SourceNotRetained branch produces whole-
root zero, destroys a parent, restarts join or implements P4–P7 by implication.

## 8. Decisive tests and remaining gate

First provenance/owning tracers must reject peer OPEN or Client input on the
bound Driver route and wrong full LocalFlow/factory, while preserving actual
carrying rejection and payload. Prove old direct read/RESET mixed intake cannot
run after binding. Then test E=1 output-before-continuation, real pending
recovery, source-only progress with retained output, exact Z retry, and
readiness-hint preservation under failed credit preflight. Index tests put the
only pending completion/join at the last Flow; no hidden F scan may do service.

Service tests must keep all thirteen categories and multiple Flows ready,
observe cursor persistence and exact visit bounds, and count contained Target
operations rather than only method entries. Include held mailbox pressure,
clear/recheck races, borrowed cancellation, failure/quarantine, absent physical
ports and shutdown-only masks. A fabricated ready bit or external test loop
that does the uncharged output work cannot prove the bound.

Before Rust scope opens, still jointly freeze: complete inward ready-domain
placement and reservation/caller migration; concrete closed-route and root
guard visibility; actual output/recovery/expiry ports and proofs; per-unit work
and physical retained Layout/cold peak. The thirteen-way selection cannot
replace these proofs. Full P3 still requires its complete process and static
Target-open/MemoryTarget/TCP/DNS ledger, not just this Owner binding.

Mencius supplied independent carrier and operation-cost source audits; Main
owns this proposal. Heisenberg is the independent Reviewer, not a co-designer.
Entry `/tmp/mini-vpn-p3-turn-contract-tuBbJz/entry.json` revalidates the last
accepted packet and1154 repository files/338 Rust/HEAD/index. No Rust, tests,
Cargo, formatting, governance, external run or commit/push occurred.

## 9. Independent selection review and next source boundary

Heisenberg reviewed the289-line proposal at SHA
`eb51db440427842796ff4bc471dc05b047010426bd0f9c074282d6bf56bad7e0`:
P0/P1/P2=0/0/0, Approve only selection/protocol; scoped Module/Clean/System/DDIA
8/9/7/8. He independently checked six links and unchanged338 Rust, companion
docs, governance and HEAD/index. He explicitly permits ports/expiry to remain
joint prerequisites, not to be skipped before executable guard/catalogue freeze.

Mencius's subsequent physical-port audit confirms the current boundary:
actor owns EstablishedLeg/LegOutboundQueue; shared output root owns Controller,
drain/page/pending lifecycle. Existing output/recovery service callers remain
the cfg(test) B-adapter; CurrentOnly has no such service calls. The lifecycle
installed/physical slot types exist, but neither CurrentOnly nor that adapter
holds them, and no root-level lifecycle settlement caller was found. Recovery
service transaction is test-only; production A-to-B installation requires the
real initial-A handoff and A/B physical parts, not the B-only test installer.
Existing actor part ports execute under first-failure critical sections.

These source facts identify the next joint placement/port/expiry/work freeze;
they do not create a production caller, final timer wiring or P6 acceptance.
Artifact `physical-port-source-audit.md` retains exact anchors. Seven-document
factual promotion passed same-Reviewer final consistency review0/0/0 Approve.
He verified audit-packet.json:315-line document,35 links, all338 Rust and
governance/HEAD/index unchanged, and inverse recovery of the reviewed proposal
SHA. No protocol/authority expansion was found. He permits only this factual
review transcription without another review; audit-final.json seals it.

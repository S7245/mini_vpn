# P3 — joint binding contract, working freeze

Date: 2026-09-07.

Status: **FULL P3 OPEN; SECTION51 OWNS CURRENT EXECUTION; FIRST REAL TERMINAL EXPIRY/ACK PATH GREEN; COMPLETE CONSUMER REMAINS DRAFT PENDING MATRIX/REVIEW**.

## 1. Current execution packet and retained design provenance

This is the single working packet for the coupled binding prerequisites.
The accepted [turn selection](2026-09-07-knife16-r7-p3-binding-turn-contract.md),
[Flow completion](2026-09-07-knife16-r7-p3-flow-completion-contract.md),
[construction direction](2026-09-07-knife16-r7-p3-binding-construction-phase-decision.md)
and [controlled wake](2026-09-07-knife16-r7-p3-controlled-wake-contract.md)
remain controlling. Open rows below cannot be implemented by choosing defaults.
Earlier conditional rows retain their provenance; section51 now owns the
accepted finite first-path execution scope. Full P3 is not accepted.

Section51 consolidates execution order under the user's workflow correction.
It replaces global all-design-before-any-Rust sequencing with an
explicit, complete Active vertical gate; all full-P3 obligations remain.

The [P3 scope](2026-09-07-knife16-r7-p3-owner-source-service-decision.md) permits
real production-shared Controller/Executor/output operations in a private
fixture. Actual actor/FIFO/lifecycle destinations must participate; a fixture
cannot implement uncharged service instead. CurrentOnly fields, parent table,
bootstrap custody, fused adoption and P6 caller integration stay in their
separately assigned stages, except the explicit section51 whole-Current
resident/service grant. Rust A1–A4 is authorized; no external gate or commit/push.

| Joint obligation | This working packet |
|---|---|
| Persistent postcommit session-expiry custody and disposition | Sections2–6 choice reviewed0/0/0; actual guarded consumer not yet frozen. |
| Whole owning turn covers actual physical critical sections | Required ordering/source facts in section7; exact ports not yet selected. |
| Same ready domain before checked construction | Sections15–18 metadata/test placement reviewed0/0/0; same-domain cold reservation/caller binding still open. |
| New OPEN reserves actual retained-F capacity before reducer commit | Sections11–14 resident-owned selection reviewed0/0/0; complete guarded caller/Interface remains open. |
| All category work, class mapping and checked service bounds | K=13 is selected conditionally; expiry charging still to reconcile. |
| Actual storage Layout and simultaneous cold peak | Open; source count, Flow positions, publisher backing and allocator requests are distinct. |
| Pure Moves, behavior tracers and exact Rust allowed files | Freeze the complete applicable vertical first, including its actual capacity/lifecycle and write scope; section51 proposes the scoped exit. Full-P3 rows remain mandatory, not all first-path prerequisites. |

## 2. Existing expiry evidence and the missing normal holder

[Reducer](../../src/resumable/session.rs) `expire_resume_grace` at1693 validates
the exact current CommittedLeg and Legless, clears its Flow/Z/replay/receive
state, and emits only SessionExpired. It does not emit per-Flow FlowFinished.
It also does not prove a physical timer deadline has elapsed.

[Supervisor](../../src/owned_upstream/supervisor.rs) at1879 clears the actual
uplink replay ownership and mints private-field ReducerSessionExpired. That
fact is Copy scalar evidence, not a unique physical capability. Its provenance
is the closed observed call from the same Supervisor. The non-Clone
OwnerTargetSessionExpired wrapper in [Executor](../../src/owned_upstream/owner_target.rs)
at295 currently travels in OwnerTargetReducerObservations.

The normal [Controller](../../src/owned_upstream/owner_session_controller.rs)
at3595 checks only fact presence, resets progress and changes phase. It does
not retain the original fact after a successful page. Cleanup failure already
has an owning observations path through Executor/Controller aborted ledgers.
The final output SessionExpired is payload-free and appears only after current
ExpireNextTarget cleanup; its non-Clone lifecycle action proves one output
installation, not the original reducer identity or completion of source drain.

Select the actual Controller session phase as the persistent normal holder.
Keep the original OwnerTargetSessionExpired there; do not duplicate its wrapper,
manufacture a FlowFinished, or reconstruct it from SessionId/phase/output.
The existing root still owns this Controller and the sole failure/output ledger.

## 3. One original fact, transferred before later cleanup

Refine the private Controller phase, not a parallel error/terminal authority.
The illustrative owning alternatives are:

```text
Active
SessionTerminal { actual expiry fact, cleanup phase }
FailedBeforeSessionExpiry

cleanup phase = Settling | CompletionOutputProduced | FailedWithCustody
```

The existing public snapshot remains value-only. SessionTerminal retains the
same fact in every subphase; a later error cannot replace it with a factless
Failed marker. Existing aborted work and exact errors remain in their original
ledgers, not inside a second fault list. No new Clone/Copy or external proof
constructor is selected. Exact Rust names/placement follow the joint freeze.

The bound observed reduction is one existing-root owning turn:

1. Preflight normal input/output/recovery exclusion and retain the exact input.
   High-water expiry preserves its actual executor identity and CommittedLeg
   checks; a generation number or output action is insufficient authorization.
2. The same Supervisor commits expiry and returns its one actual observation.
   Keep that observation in the owned result/aborted page across every error.
3. On the bound route, the SessionExpired effect no longer runs the legacy
   ExpireNextTarget loop before Controller receives the fact. It returns the
   real observation and an empty output page, with all actual Target/Flow/source
   records still owned. It does not mint a continuation waiting for inbox drain.
   This reducer's sole visible effect is audited; an unexpected effect/fact
   combination retains its real batch and quarantines instead of dropping it.
4. Controller validates the fact's session/generation against this actual
   Executor, then moves the original wrapper from the observed bundle into
   SessionTerminal. Progress reset is applied once at that transition. The
   already-entered root turn prevents another input/production unit between
   reducer commit and this retained state; interruption stays nonzero with the
   original result/fact, never normalized back to Active.
5. SessionTerminal immediately excludes live source production/open/write
   admission. It requests only the actual later settlement work. An empty
   effect queue is not session completion and does not erase the retained fact.

Logical cleanup is now distinct from effect continuation. Unbound Legacy tests
remain separate; effects-only/raw methods cannot be used to discard the fact
on the bound route. The closed caller audit must cover every reachable expiry
producer, not just the normal apply_event_observed path.

This proposal does not infer a timer due event. The existing OwnerLeglessHighWater
already holds executor identity plus the original ResumeGraceExpired event,
but its effects-only consumer and Controller-held high-water still need a
guarded observed consumer. Physical timer registration/deadline ownership must
be named separately; adding a CurrentOnly timer registry is not authorized.

## 4. Borrowed proof authorizes real retained-resource disposition

Controller passes a borrow of its retained fact inward during the complete
owning operation. No caller supplies another Executor's observation. This
borrow is not a getter/receipt escape or a newly transferable cleanup token.
Executor still owns its original Flow/source/factory and checks that pairing.

For late Driver DATA/LocalReset(TargetFailure)/LocalClose, validate the entire
LocalFlow against the actual retained source: session, FlowId, direction and
authority_id, plus the existing factory/permit origin and cold-paired route.
The model no longer contains that Flow, so this disposition occurs before
normal reducer validation. RetiredFlow/UnknownFlow is not a carrying pressure
return. Unexpected class/identity keeps the actual command and quarantines.

Matched disposition drops real staged DATA/permits or the exact zero-byte
Control command once, then settles its existing transit position. It emits no
ACK, RESET, FlowFinished, progress observation or Z tombstone. Replay ownership
was released by Supervisor's actual expiry branch; it is not released again.
Unknown/non-carrying errors preserve their original ownership and nonzero
disposition; a transit counter cannot manufacture the missing command.

SessionTerminal disables all bound production at once without an F walk.
Actual source admission revocation/destruction is then one named Flow stage,
under its real admission guard and with the original source still retained.
No external source producer or off-turn normal releaser is permitted by this
binding. Closing a producer does not prove receiver drain: the two physical
Driver lanes still use their charged source units, cells, mailbox and inbox.
Only actual end-of-stream after closure proves a retained lane drained.

Pending write, completion, deferred sink work and Target outcome stay in the
existing Flow record until their exact terminal disposition. Already-produced
commands are not staged again. A source-less/Target-less failed Open uses the
accepted proven-absence branch, not invented drain/join evidence.

## 5. Target completion cannot delete the Flow record

The old ExpireNextTarget at5227 first retires old Target tombstones, then calls
expire/join/retire and removes the Flow. Bound service must separate these
actual operations from record deletion:

- Expire one actual Target at most once, retaining its returned terminal kind
  and any precise failure. Do not assume the kind from a desired cleanup plan.
- A real Pending join retains the same Target, registers its named Join
  interest and waits for an actual eligible retry; it is not normal completion
  or automatic root fatal. The immutable same-Target terminal state prevents
  another expiry/reset/close call on retry.
- Successful join and exact Target-tombstone retirement settle that Target's
  presence, not its Flow/source record. Already joined tombstones from before
  session expiry still require their real kind/owner-correlated retirement.
- Release pending sink/completion work only under the retained expiry fact.
  Preserve expected pressure or unknown ownership rather than restoring a
  live Flow after reducer expiry.
- Delete the record only after source presence-sensitive revoke/two-lane drain,
  exact transit/current/completion/work disposition, Target disposition and
  actual publisher unregister/reclamation are all settled. Retiring keeps F.

Final completion production is one ordinary output-admission turn after those
actual positions and remaining effects are settled. Produce SessionExpired
once, retain the fact and change to CompletionOutputProduced. This phase means
the one real output is in Controller/root turn/output custody, not installed
timer state or whole-root zero. Root pressure/rejected-page/error must retain
that same output; another turn cannot synthesize it again. Controller phase
alone cannot prove root output/lifecycle/actor/endpoint/session zero.

The final output cannot be required to authorize the cleanup that precedes it.
No continuation may wait on its own excluded mailbox, publisher release or
Target join. Output admission still uses the actual existing guard.

## 6. Cold classification and charging consequences

Preserve the proven cold live-Target and pending-recovery callers. Cold is an
exclusive interval, not a pristine session. Their exact original Executor,
clock, plan and same-domain reservation must return on modeled rejection.

No current test establishes checked construction from an Expiring/Expired
Executor. Current new_checked only validates capacity and always initializes
Active; capacity success is not that business-state proof. In this proposal,
an expired model without the original required expiry custody is an explicit
precommit binding rejection with all inputs returned. Do not silently relabel
it Active, infer the fact from its phase, or call this pristine-only admission.
If an actual supported terminal cold caller is found, this classification must
be revised with its real transferable custody before Rust; no compatibility
default or guessed proof is allowed.

Target tombstones are bounded by Z, whereas retained Flow/publisher positions
are bounded by F. Session expiry makes both populations serviceable. Category13
cannot hide an uncharged Z sweep or assume Z<=F. The complete joint catalogue
must charge both populations and final output, with persistent fair selection;
any extra subvisit/category revises the earlier K/delay formula. No new Z ready
lease or command queue is selected by this observation. Each actual expiry,
join, retirement, lane poll and disposition is counted where it executes.

Normal session expiry is not automatically process shutdown. The final joint
mask/class design must distinguish these from the separately accepted shutdown
migration with its new scheduling generation and exact residual rebind set.
Neither a class bit nor a mask creates physical timer/parent custody.

## 7. Whole-turn physical guard ordering still to freeze

The actual existing B-adapter Continue call enters actor first-failure exclusion
before continue_and_settle. New production-shared service must contain that
actual return path: Owner-local publication remains metadata-only until the
actor guard, as well as source/Target/ready guards, has exited. Notify occurs
afterward under the accepted same-first-fault/single-waiter contract. Merely
moving finalization after TCP admission unlock is insufficient.

No provider, callback, optional actor-slot Interface, borrowed arbitrary Driver
or success-only physical-port stub is selected. The joint packet must still
choose real output/recovery/lifecycle inputs and all rejection/error returns.
A private fixture can supply real resources; it cannot be the Implementation
of the production service transaction. Actual CurrentOnly/P6 wiring is later.

## 8. Falsifiers and evidence status

Required future tests include: wrong/recovered high-water exact return before
commit; fact retention on postcommit failure; late zero-byte RESET/CLOSE and
staged DATA in every actual position; full LocalFlow/factory mismatch retention;
Target Pending join without repeat expiry; Z>F old tombstones; no continuation
but nonzero transit; publisher release after join; one final output under
pressure; preserved live/recovery cold inputs and exact-return expired input
without a fact; Notify outside actual actor exclusion.

All use the real observed/shared owning operation. A handcrafted expiry fact,
timer-due boolean, fake ACK, arbitrary external Driver or uncharged fixture
cleanup cannot prove these requirements. Timer due registration and the actual
guarded high-water caller remain explicit prerequisites, not covered by the
postcommit fact itself.

Mencius independently traced the current expiry producer/consumer/cold chain;
Main inspected its source bodies and owns this proposal. No Rust test or new
runtime failure is claimed. Entry artifact
`/tmp/mini-vpn-p3-joint-binding-m6KfFp/entry.json` verifies the previous accepted
packet and1155 files/338 Rust/HEAD/index. Heisenberg independently reviewed the
246-line draft at SHA
`ce4973afd8f5b94fd6166822ff4c109b15562eadaaa0880904593dd7b573fe70`:
P0/P1/P2=0/0/0, Approve only sections2–6 selection and section7's guard constraint,
scoped Module/Clean/System/DDIA8/9/7/8. He checked nine links,338 unchanged Rust,
six companion docs, governance and HEAD/index. No necessary structural
optimization was found. Actual guarded consumer/ports, masks, work and Layout
remain open; this review does not approve the joint freeze, Rust, P6 or full P3.

Final seven-document consistency review first found one P2: the result
overstated dual-private access as all56 tests instead of the12 metadata
descendants. The sentence was corrected without changing this contract.
Same Heisenberg's closure is P0/P1/P2=0/0/0, Approve only that repair and
document consistency. He verified38 links and338 unchanged Rust/governance/
HEAD/index, and permitted this factual transcription without recursive review.
`/tmp/mini-vpn-p3-joint-binding-m6KfFp/audit-final.json` seals the packet;
the joint freeze and implementation grants remain explicitly open.

## 9. Source-derived inward metadata and test closure

Mencius's initial source audit identifies the current dependency closure:
ReadyClass/ReadyProcessPlan/Error and checked arithmetic; SendLane/SendCursor;
ReadyFault/FailureGate/Key/Phase/Cell/Slot/Tree/Shared, CapacityWake, ReadyClaim,
ReadyIndex and their full implementations. The cursor value stays with actual
SessionCommands ownership. Commands/two cells remain under their selected
inward Executor owner, not inside neutral ready metadata. Actual outer resident,
dispatch, visit custody and park registration remain composition responsibilities.

Current scheduler59 imports parent ReadyProcessPlan; scheduler429 imports
Layout<super::CommandResident>, although outer601 already checks that type.
The Waker array checked at430 is also actually allocated outside. Exact check
ownership and error order must be frozen before movement; do not delete or
reorder them under a claim of literal text-only Move.

The56 current tests are44 outer tests and12 metadata-descendant tests. A later
12-item audit finds all12 depend on outer custody; nine additionally depend on
private metadata. The other three use existing metadata operations. Moving
them to an inward sibling breaks their outer access and nine dual accesses. Import
rewrites alone do not fix locality. A separate test-closure strategy must be
reviewed before Rust; raw getters, re-exports or visibility diffusion are not
automatic repair authority. No current raw-source assertion on these two
production files was found; general provenance/governance still applies.

Cold allocation evidence also constrains the future Layout: original command
Vec backing remains live; outer allocates residents/initial/wakers/identities
Vecs; metadata allocates N slots and four trees, each with separate words and
levels Vecs; each levels Vec reserves WORD_BITS Range entries. Notify/Shared
Arcs, the root OwnedNotified Box and R<=N capacity Waker allocations follow.
Original command, Waker and identity backing coexist during final transfer.
The input Vec is length-checked, not capacity-bounded. Requested reservation
size is not allocator byte usage; no numeric physical envelope is proved yet.

## 10. Retained F needs admission before new OPEN commit

The future binding cannot equate model live count with physical F occupancy.
Reducer finish_flow at3002 removes live Flow while the accepted Retiring phase
must retain old publisher backing in its F position. Reducer receive_open at2024
then checks only its live count, commits the new Flow/request/authority, and
emits PeerOpenRequested. Executor4501 retains a real record/completion even
after failed Target admission. Source factory gate257 checks closed/session/
direction/authority high-water, not a reserved physical F position.

Therefore an F-full Retiring population can leave a new committed request with
nowhere to retain its required record if physical admission waits until the
effect. This is a source-proved future-cutover counterexample; Retiring is not
implemented today and no new runtime failure is claimed.

Duplicate OPEN must still classify first: tombstone replay, completed live
replay and pending live no-op occur before the model's new-flow capacity check.
Do not require another physical position for those operations or duplicate
their classification outside the model. The existing carrying rejection holds
the exact boxed command, but no same-domain physical-F precommit seam currently
feeds it. Existing prepared OPEN is Client-local, not Owner-peer admission.

The joint contract must select that complete Owner precommit reservation and
its rollback/commit ownership before binding. No hidden F+1 record, Z substitute,
discarded failed-Open completion, factory high-water reset or new generic
callback/provider is selected. Section10 freezes the counterexample and required
property only, not its Interface or a new Rust scope.

Exact source-audit anchors and current allocation order are retained in
`/tmp/mini-vpn-p3-joint-binding-m6KfFp/metadata-and-admission-audit.md`.
The unresolved joint rows are implementation stops, not an impasse declaration.

## 11. Selected OPEN locality: the actual Flow position owns admission

Select the existing Executor Flow settlement owner, not an additional queue of
reservation-bearing work. Its bound F positions include precommit reservations,
committed-but-not-yet-started OPEN, opening/live/finishing records and Retiring
publisher backing. A position is reusable only at its actual release edge.
The sum never exceeds F; model live count and a copied free-count do not prove it.

The alternative is to carry a new F capability through observations, ordinary
effects, pending/aborted pages and finally Flow records. None of those ledgers
owns such a capability today. That alternative adds a second chain of transfer
and rollback rules for resources ultimately settled by the same Flow owner.
Resident ownership keeps the request/phase and physical reservation local.
Existing pending/abort work can identify that resident; it cannot also own a
second copy of its OPEN or free its reservation independently.

This is a reviewed internal transaction selection, not an approved Interface.
Exact type names, Module scope and caller-operation visibility must still be
included in the joint freeze. It adds no generic callback, provider, raw Driver,
mutable source-factory escape or new global owner. The pure model depends on
neither Executor nor ready metadata.

## 12. One classification, two distinct commit edges

The required new inner prepare operation shares the model's actual peer-frame
validation and OPEN classification with its existing route. It preserves the
order: current CommittedLeg, Active phase, frame generation/protocol, Owner
role, tombstone duplicate, live duplicate, monotonic ID, live F and ID limits.
Do not repeat that classification in Supervisor/Executor or probe a snapshot
and later call receive_open again. A named non-Clone prepared state holds the
same model's exclusive mutable borrow until commit or rejection; replay/no-op
and genuinely-new branches are distinguished by that preparation.

The caller supplies no scalar classification certificate. A new branch exposes
only its correlated, already-materialized preparation input within the closed
transaction; the actual candidate Flow/request/next IDs stay model-owned.
Pure request values can be materialized before commit, but copying those values
does not duplicate a source, publisher, socket or reservation. Model mutation
cannot interleave while the prepared state is live. This is not authorization
to export a reusable LocalFlow constructor or a live-model getter.

For a genuinely new OPEN, use this order inside the existing owning turn:

1. Retain the original input at its current owning site. Model preparation may
   borrow it; it must own its prepared values before that input is moved. Every
   modeled preparation error returns that same input, not an event clone.
2. Reserve one real F position and one O attempt plus the already-required
   ready/request/result/fault/timer/resolver/socket/command storage. O includes
   Reserved and CompletionHeld, not just sockets currently connecting. Prepare
   modeled fallible backing before request admission. No factory consumption,
   Target begin, DNS, socket operation or notification occurs here.
3. Install the complete candidate and reservation in the actual F resident
   before model mutation. The original input remains root/transaction-owned
   once, and the already-entered failure authority inventories this resident.
   Expected precommit rejection cancels the inner preparation and returns every
   acquired reservation and the original input, without advancing model or
   factory high-water. Unknown unwind is not that expected rollback branch.
4. Consume the same preparation to commit the model's request/Flow/IDs, then
   immediately publish that resident's RequestAdmitted phase. No Target call,
   user callback, new validation or modeled fallible reservation belongs in
   this gap. The candidate is already resident-owned if interruption prevents
   the final phase publication: retain it and its reservations in quarantine,
   never infer that model mutation did not happen from a missing return value.
5. Only the admitted resident may consume the one source-factory authority and
   enter the accepted PendingTargetOpen transaction. Source/Target presence is
   explicit. A factory invariant error or a failed cold begin retains the real
   request, exact error and reservations; it cannot refund F by pretending the
   reducer never admitted the request. Normal Target failure uses the original
   reserved completion and actual presence-sensitive disposition.

RequestAdmitted is not Target installation or application acknowledgement.
Target finish/installation plus exact PeerOpenResolved storage retain their
separate final infallible edge in the accepted Target-open amendment. No source
or socket is duplicated between resident phases. A slow open releases the
ordinary effect continuation as that amendment requires; O pressure cannot
park an effect head waiting for a completion that the same head excludes.

Reserving O before request admission avoids an unbudgeted postcommit attempt
waiting room. Duplicate/tombstone replay and pending duplicate no-op reserve
neither F nor O; they still require their actual ordinary output capacity.
The original normal duplicate/effect semantics must survive shared preparation.

The accepted allocator fault scope remains unchanged: all modeled fallible
storage is prepared before the relevant commit; std/channel/Arc allocator
abort is not a recoverable OOM promise. Existing BTreeMap insert, Target clones,
effect Vec/VecDeque and output growth are not allocation-free or proved bounded
backing merely because counts are bounded. The complete Layout/cold-peak audit
must inventory them. This selection does not order a global model-map rewrite.

## 13. Actor pressure is a real caller obligation, not a local-inbox shortcut

Owner OPEN currently arrives through OwnerSessionActorEvent: an owned boxed
event plus its actual QuinnOwnerActorArrival. Controller3232 checks phase,
clock, recovery and continuation, then unboxes the event. Supervisor1295 clones
the event for retry and1241 reconstructs a Box; this proves a carrying command
value, not the original Box allocation. The new precommit path must retain the
original event allocation and arrival witness, without reconstructing either.

The actual output root at continuation691 enters its turn before applying the
actor event and settles every returned error as controller failure. Therefore
adding a new Executor capacity error alone does not implement recoverable
admission. The joint guarded caller must distinguish only the proven precommit
new-OPEN F/O pressure from existing actor clock/provenance/protocol/unknown
errors; those errors are not globally relaxed.

Select root-owned retained input at the existing bounded current-input site,
charged once to R_cmd. It is not another inbox or a replacement arrival stamp.
After a proven no-commit rejection, settle the empty-output turn explicitly;
do not retain an OwnerTargetResume or leave an entered output guard blocking
internal completion/retirement. Existing Flow and TargetOpen settlement must
remain eligible while this input waits. No later actor frame may be admitted
out of the transport's required order, and no frame may be dropped to skip it.

Retry requires actual F/O release notification and its generation/epoch
clear/recheck contract, not periodic retry or a Target readiness hint. A held
actor OPEN is not a Driver Control command; do not route it through the local
Driver inbox to reuse its ready bit. This adds a named retained-input retry
obligation to the joint masks/work catalogue, so K=13 is not promoted here.
The actual port, retained-input storage, budget and ordering proof are not yet
frozen. In particular, demonstrate no dependency on a later excluded actor
frame before claiming forward progress under pressure.

## 14. Required admission falsifiers and acceptance boundary

The future joint tracer set must show:

- F=1 with the old Flow physically Retiring and model-live zero: new OPEN
  pressure leaves model/request/factory IDs unchanged and returns the original
  input, then actual publisher reclamation permits exactly one admission.
- F and O full: tombstone/live/pending duplicates retain their old semantics
  without another position; wrong Target and wrong leg still reject correctly.
- Each modeled reservation failure returns the exact original input and all
  earlier reservations; no source, Target, timer or wake was published.
- Unknown interruption immediately before/after request admission retains the
  same candidate/resident/input survivors with sticky nonzero F/O custody.
- Deferred work and abort pages have a bidirectional relation with their actual
  resident; neither missing work nor a second owning reservation can pass.
- Failed source admission, failed begin and delayed Target completion keep the
  same F position and only release O at the accepted attempt settlement edge.
- Held actor OPEN preserves its original event allocation and arrival; real
  capacity release enables retry while already-admitted completion/retirement
  progresses without an excluded later-frame dependency or hidden F scan.
- Cold live-Target/pending-recovery construction retains its supported state;
  only real new attempts acquire O. No blanket pristine-state restriction.

Mencius's source-only audit found no existing prepared Owner admission or F
custody in observations/pending/abort work. Main additionally traced the real
actor wrapper and output-root failure path. These are source findings, not
test failures or completed implementation. Same Heisenberg reviewed sections11–14
and the table change: first P0/P1/P2=0/0/1 for a wrong clone-source anchor
(1288 instead of1295), then0/0/0 Approve after that sole correction. Selection
scores are Module/Clean/System/DDIA8/9/7/8; exact caller/storage/scheduling/Layout
proofs are the stated gaps. The479-line reviewed SHA is
`b51237d0ed5a176b2d3cb6cefdc406e5f7f2ede7aa802d38c6f53b7acb9a101e`.

Mencius's independent liveness audit finds a concrete cycle if old model
live-F pressure were changed into ordered held retry: OPEN(B) precedes the
RESET/final ACK(A) needed to free A. This is excluded by the selected order,
not solved by it. Model-terminal Retiring and O completion did not reveal the
same necessary later-frame dependency, but actual internal completion/grace/
output eligibility and release-driven retry remain unproved joint obligations.
No queue, reordering or Active-session expiry default was selected as a remedy.

Artifact `/tmp/mini-vpn-p3-open-admission-sBPpNl/` holds entry/audit-draft/
audit-anchor-repair plus source-audit.md and liveness-audit.md. Reviewer verified
the inverse anchor repair,338 unchanged Rust/governance/HEAD/index and allowed
limited compact-doc synchronization before final consistency review. Remaining
joint rows, exact Interface/ports/Layout, Rust, P6 and full P3 stay unaccepted.

Final same-Reviewer seven-document consistency review: P0/P1/P2=0/0/0,
Approve only consistency. Seven fingerprints,38 links,338 Rust/governance and
HEAD/index independently verified; old model-pressure and allocator-abort
limits remain intact. Reviewer allowed only this factual joint/result
transcription; `audit-final.json` in the artifact directory seals the packet.

## 15. Proposed placement: inward metadata, outer process policy

Do not literally move the whole current dependency closure in section9: that
would drag ReadyProcessPlan's class budgets/service policy inward. Preserve
the accepted outer process classes/budgets/admission/park, and remove the
reverse dependency through a preparatory, behavior-preserving abstraction.

Select one private default-path `owned_upstream::owner_ready` Module for the
existing complete ReadyIndex/ReadyShared/failure/slot/cell/tree/publisher/claim
Implementation, SendLane/SendCursor and a checked capacity-geometry value.
It owns neither commands/Driver/Target nor outer residents/visits/park waiter.
The actual mutable process selector remains root-owned; publishers are still
non-owning. Moving the Implementation does not create a second selector or
allow Controller to acquire arbitrary mutable ready state.

ReadyClass, ReadyProcessPlan/Error and all budget/service-delay policy remain
outer. Their plan contains the canonical inward geometry rather than copying
its N/process/class capacity fields into a second geometry authority. The
geometry is a checked immutable value, not a domain identity, reservation or
permission to create another publisher. It contains only the N-dependent
capacity facts used by both metadata allocation and outer service arithmetic.
Fixed class position mapping remains exact; no caller supplies an arbitrary
class-capacity array or asks inward code to execute its process policy.

Preserve current plan error precedence: zero sessions, zero class budgets,
process-capacity arithmetic, total budget and per-class service bounds. One
possible nonzero input representation is NonZeroUsize; it does not reorder
checks or add a default plan. Pure value access to geometry is not a raw
Flow/Driver/state getter. Exact names/visibility/signatures still require the
joint symbol freeze; no Rust grant follows from this selection proposal.

### Foreign Layout checks are not text-only movement

Current ReadyIndex::new has one caller: OwnerServerRoot::from_commands at629.
That caller checks Layout<CommandResident> at601 before allocating residents;
it successfully reserves Vec<Waker> at615–618 before reaching ReadyIndex.
The inner repeated CommandResident/Waker Layout checks at429–430 cannot first
fail on that successful caller path. They describe outer-owned storage, not
inward metadata. Remove those two dominated checks only in the characterized
preparatory step, without moving new checks ahead of earlier Reserve errors.

All remaining inner mask/count/slot/tree checks and allocation order stay
unchanged. Geometry substitution preserves the same numeric values. The outer
input Vec, four scratch/resident vectors, actual park registration and Waker
allocation ownership remain unchanged. New cold consumers do not thereby gain
a proof of their own foreign storage; they must validate their actual Layout
in their owning constructor. This is no allocator/RSS or full cold-peak claim.

## 16. Proposed test closure: private exact fixtures, not state getters

The finite audit is12 tests: three ready_dispatch, six publisher_fatal and
three child tests. All12 belong to the actual outer-root behavioral surface:
they run real from_commands/poll_ready_unit/mailbox send/receive/model reduction
and inspect real command custody. None should be moved inward wholesale.
Three need only existing ready operations; nine additionally need private
metadata assertions/injections. The section9 count is refined accordingly;
earlier review reports retain their historical wording as evidence, not a new
claim that all12 directly read private metadata.

Keep these at the outer root's private test tree after preparatory convergence:
Full Box addresses, Fresh/Returned/Unknown/Vacant, actual request/event identity,
resident faults/quarantine and the same in-flight claim address/slot/lane.
Actual enqueue/dequeue/notify/park order, warm receipts, repeat counts and
teardown remain in the same scenarios. Do not replace them with isolated
ready-bit tests or a final check_root error assertion.

Move only metadata-owned arrangement/assertion recipes and the three native
metadata-aware Wake implementations into the metadata Module's private
`tests/` tree. Use fixed scenario fixtures with opaque captured key/revisions
and explicit assertion stages. They must preserve:

- source revision exhaustion separately from process revision exhaustion;
- actual mutex poison by unwinding with its real guard, never fail_root alone;
- exact source/process phases, key/revisions, word/mask/cursor and tree bits;
- epoch dirtying without normalization and real poison remaining in place;
- callback-time first cause and successful try_lock, with guard release before
  the intended non-poisoning panic; poisoning callbacks keep the guard;
- equality of the actual borrowed ReadyClaim with the captured metadata,
  without exposing or rebuilding its private key or copying its authority.

The fixture owns no command, Root, scheduler lease or second gate. A native
Wake keeps the same Weak<ReadyShared> and exact callback-count/Arc lifetime
semantics as the current recipe; test observations are not new runtime state.
Do not introduce generic closures, raw Mutex/Arc/field getters, reflected
metadata snapshots, a field-mutation language or extra production hooks.
Outer mailbox-only Wake/Drop fixtures stay outer with their real receipts.

Keep the production seam as only `#[cfg(test)] mod tests;`. Test-only inherent
entry methods on ReadyIndex/ReadyClaim may be implemented inside that private
test tree, returning an inferred opaque fixture whose fields/path remain
private. Its declared visibility may cover the common owned_upstream test
caller scope to avoid private-interface errors; this is not a public tests
Module or re-export. No production field/method visibility is widened for
tests. Exact finite entry/assertion operations must be enumerated and reviewed
at the Rust freeze; a universal inspection/mutation Interface is forbidden.

The private-child/inferred-return technique is already used by the accepted
hard-terminal extraction, but that precedent alone does not approve these
new test operations. Tests must demonstrate the original causal observations
at the same points, including poison readback without clearing it. Any recipe
that cannot preserve those conditions stops this migration; it is not deleted
or weakened to make the Move pass.

## 17. Preparatory sequence before the mechanical Move

All steps remain gated on the complete joint freeze and exact Rust scope:

1. Characterize current tests/plan/construction and record the exact12 symbols,
   fixture closure and injection/observation map. Separate the three existing-
   operation tests from the nine requiring metadata-private recipes.
2. Converge those recipes behind private test fixtures in the current metadata
   location; no production changes. Preserve assertions and all real causal
   operations, then run the exact tests/repeats and independent review.
3. Move the12 complete root scenarios plus their outer-only fixture closure to
   the outer root's private tests, while metadata fixtures stay inward. Record
   changed fully-qualified test routes and lock their unique inventory; do not
   claim original FQ names are unchanged or run a zero-test filter as evidence.
4. Separately introduce canonical geometry and remove the two proved-dominated
   foreign Layout checks. Characterize validation/error precedence, exact cold
   input return and all plan boundaries. This is deliberate preparatory
   refactoring, not a literal unchanged-block Move.
5. Mechanically move the complete prepared metadata Implementation and its
   private tests to owner_ready. Root imports inward directly; no re-export,
   parent pass-through, duplicated geometry or second failure gate. Keep all
   fields private and restrict necessary operations to their actual common
   caller scope. Perform symbol/body/route checks plus focused/full regressions,
   provenance, growth/fmt/diff/check and independent review.

All new files must obey normal production/test/index limits. No oversized
inventory entry may be added; only Integrator handles an actual shrink ratchet.
SessionCommands movement/binding and same-domain cold reservation are separate
joint obligations, not silently bundled into this metadata Move. Production
behavior changes start only after their own complete allowed-files/symbol freeze.

## 18. Metadata selection boundary and evidence

This proposal resolves placement and test strategy, not the full binding
Interface, cold caller migration, actual reservation, legal masks or Layout.
Metadata cannot import server_root or its process-plan/command types. Any
need for such a reverse reference, a new production getter, weakened fault
test, duplicate owning state or altered error precedence rejects the proposal.

Mencius completed the finite12-item source audit without tests or writes;
Main read the private-field/callback recipes and sole constructor call. The
three existing-operation tests still require outer custody access, so placing
all12 in the inward test tree is not the solution. Same independent Heisenberg
reviewed the651-line draft, SHA
`be44ece9c23059ec7956775811dffa6416ad903a001e6f001cf115468efe9f31`:
P0/P1/P2=0/0/0, Approve only locality/test-closure selection;
Module/Clean/System/DDIA9/9/8/8, no necessary structural optimization.
Finite fixture operations and assertion/lifetime mapping, exact geometry/error
precedence and actual cold binding remain explicit freeze obligations.

Artifact `/tmp/mini-vpn-p3-ready-locality-lXGH09/` holds entry/audit-draft and
the finite test-closure audit. Reviewer verified all12 symbols,9 links, six
unchanged companions,338 Rust/governance/HEAD/index. A further read-only audit
confirmed the sole constructor and both Layout dominators; of nine outer
process-plan tests, only two have four capacity-value reads to migrate.
Final same-Heisenberg seven-document consistency review: P0/P1/P2=0/0/0,
Approve only consistency. Seven fingerprints,38 links,338 Rust/governance and
HEAD/index verified; inverse recovery matches the approved651-line draft and
the historical result prefix is unchanged. Reviewer permits only this factual
joint/result transcription followed by immutable `audit-final.json`, without
recursive review. This adds no Rust, P6, public TUN or full-P3 acceptance.

## 19. Finite test-operation contract proposed for the selected placement

This refines section16's nine metadata-private scenarios, not a production
inspection Interface. The three existing-operation scenarios need no new
metadata recipe. All actual polls, mailbox operations, receipt reductions,
outer custody assertions, repetition counts and close operations stay outer.

Each row below defines fixed arrangement/observation operations at the original
points. These are semantic operation labels, not an executable Rust signature
grant. Use separate named stages, not arbitrary slot/field/phase parameters,
generic closures or a mutation language. A common implementation may share
identical private assertions; it cannot merge observations across a causal
operation or strengthen/weaken the tested contract during a pure Move.

The first six symbols are in [publisher_fatal.rs](../../src/owned_upstream/quinn_leg_composition/owner/server_root/tests/ready_dispatch/publisher_fatal.rs).
Links follow current outer scenarios; numeric spans below retain their original
preparation-baseline meaning, not the post-Move source line numbers.
S means sources[0], P means processes[SESSION_CLASS]. Slot numbering retains
each existing fixture's actual vector order; B is slot0 in root_with_full_b,
while A is slot0 in the two direct from_commands source-unwind fixtures.

| Exact test symbol | Fixed operations and observation points | Actual claim crossing |
|---|---|---|
| publisher_revision_exhaustion_closes_admission_before_another_resident_send | After B Waiting: mask1/S-P Idle, arm only S MAX (66–74). After real dequeue, before next poll: S MAX/Quarantined (78–80). | None. |
| process_revision_exhaustion_is_published_before_root_selection | Waiting: mask1/word0/S1-Idle/P-Idle, arm only P MAX (150–160). Dequeue before selection: S2-Published/P MAX-Quarantined/word1/mask1 (165–175). | None. |
| slot_poison_is_published_before_root_selection | Unpoisoned (241), existing gate check, real guarded panic with original payload (243–251), poisoned (253), existing gate/dequeue checks, still poisoned after close (289). | None. |
| poisoned_metadata_source_unwind_retains_claim_and_mailbox_disposition | Capture original key/S-P revisions (339–346); create original poison Wake (347). At every retained-claim round compare capture (389), then read actual poison without clearing it: RunningClean/key/revisions/word1/mask0/cursor1 (403–423). | Borrow each actual Running claim for that comparison only. Outer keeps address/slot/lane assertions. |
| first_published_poison_survives_later_process_revision_exhaustion | After both Waiting: check both slots (486–495), arm only A P MAX, really poison B (498–510). Between real B/A dequeues verify A still Idle/unpublished (515–524); after A dequeue verify its S2-Published/P MAX-Quarantined/word1/mask1 (529–538). | None; first-cause checks use existing check_root. |
| publisher_fault_during_send_preserves_admitted_completion | Arm B S MAX (608–615), capture A Published key/revisions (618–628). After A send returns, before receipts leave: B quarantine (655–657), same A identity/revisions now Idle/word0/mask0 (665–680), tree only B (682–684). | None; do not invent an intermediate claim observation. |

The remaining three tests retain their exact suffixes and current source:

| Exact test symbol and source | Fixed operations and observation points | Actual claim crossing |
|---|---|---|
| committed_publisher_fault_survives_root_notify_unwind — [notify_unwind.rs](../../src/owned_upstream/quinn_leg_composition/owner/server_root/tests/ready_dispatch/publisher_fatal/notify_unwind.rs) | Arm B (66–73); create original Wake (75). After real Notify unwind: no poison, S quarantine/P Published/word1/mask1/tree B (97–112), final no-poison check (173). Preserve all callback-time assertions in section20. | None. |
| fatal_during_cached_waker_replacement_prevents_repark — [park_fatal.rs](../../src/owned_upstream/quinn_leg_composition/owner/server_root/tests/ready_dispatch/publisher_fatal/park_fatal.rs) | Arm B (70–77). After external old-Waker drop, before replacement: capture even epoch/tree empty (103–108). After real replacement/dequeue: same epoch dirty, tree B/S-P/word/mask (126–142). After existing park_epoch rejection and every repeat, dirty value unchanged (147/160). | None. |
| healthy_source_unwind_quarantines_only_its_resident — [source_unknown.rs](../../src/owned_upstream/quinn_leg_composition/owner/server_root/tests/ready_dispatch/publisher_fatal/source_unknown.rs) | Capture both Published key/revisions (56–68), create original healthy Wake (69). Preserve unrecovered, locally quarantined A, and normally sent B stages separately: phases/key/revisions/dormant cells/word/mask/cursor/tree (138–173,189–207), final all-unpoisoned (247–252). | Only the actual pre-recovery Running claim (100–107). After recovery outer proves Idle; no empty or synthetic claim. |

Only the two named source-unwind tests pass an actual &ReadyClaim. The
observation compares its private key/revisions against the original capture;
it cannot retain, clone or reconstruct the claim. Other tests must not acquire
an extra claim merely to make an assertion helper convenient.

## 20. Short borrows and exact callback lifetime, not a generic fixture owner

An observation value may encapsulate only the already-existing expected
key/revision tuples and epoch values. It holds no &ReadyIndex across a mutable
Root poll. Every observation short-borrows the actual index, and where required
the actual claim, at its original assertion point. No long-lived guard, command,
lease, extra strong/weak reference, Waker or heap backing is introduced.

Native Wake construction moves the existing Arc into the same outer local;
do not add a fixture wrapper that also retains it. The outer caller can infer
the concrete type and perform the original Arc clone/Waker conversion and
strong_count check. The callback's existing Weak<ReadyShared> remains its only
metadata reference outside invocation; upgrade exists only during invocation.
Private callback-count assertion operations replace private counter reads at
the same points: fixed not-called/called-once assertions with the original
SeqCst load, not a general counter getter or expected-value mutation API.

| Native Wake | Callback semantics kept intact | Original ending kept intact |
|---|---|---|
| PoisonOnFirstReceiveWake | Both wake and custom wake_by_ref remain; only first callback upgrades Weak, obtains actual try_lock guard and panics with that guard held. Original literal panic/counter checks remain. | Actual receipt, noop empty polls, drop Waker, trigger strong_count1/calls1, drop trigger, then close. |
| PanicAfterCommittedFault | Only wake is implemented. First call proves committed RevisionExhausted and unlocked slot/S quarantine, releases guard, then original literal panic. | No explicit drop or strong_count1 assertion exists. Keep the original Context/Waker/trigger scopes through repeated polls, receipts, close and final checks. |
| PanicWithHealthyMetadata | Only wake is implemented. First call proves Open and unlocked RunningClean S/P, releases guard, then original literal panic. | Noop empty polls, drop Waker, trigger strong_count1/calls1, drop trigger, then close and final no-poison check. |

Do not unify these Wake implementations: the local standard library's default
wake_by_ref clones the Arc before invoking wake; the first custom implementation
does not. Neither add a new custom override to the latter two nor remove the
first one's override during movement. Preserve original panic payloads and
outer catch_unwind scopes; fixture callbacks are not production failure hooks.

The other callbacks remain outer. DequeueDuringSend retains its real receipt
and drop(trigger_waker) followed by Arc::try_unwrap; DequeueOnCachedWakerDrop
must have no external strong reference after its original drop point, so the
next real cached-Waker replacement triggers the dequeue. A shared cleanup
wrapper could change that causality and is forbidden. No fixture Drop performs
cleanup, registration reset, poison clearing or synthetic settlement.

Private inferred return alone does not override Rust privacy: only test
fixture type/operation declarations get the common test-caller scope selected
in section16. Their module path and fields remain private. Production fields
and operations do not widen for testing. Constructor/assertion methods must
return independent observation values or the existing native Arc, never a
borrow tying the fixture to Root for the rest of the scenario.

## 21. Finite-contract evidence and remaining gate

Mencius's second finite audit found no inherent need for additional production
ownership, but this is conditional source feasibility, not compiled signatures.
Main inspected the original callback bodies, relevant assertion/custody blocks,
teardown and local alloc::task::Wake default implementation. The two exact
claim crossings and three differing callback lifetimes are preserved above.
Same Heisenberg independently approved only sections19–21, P0/P1/P2=0/0/0,
Module/Clean/System/DDIA9/9/8/8; no necessary structural optimization. Reviewed
763-line SHA `23b33cb6fc43a0025ba793408e2480bff757e091b4bad4a5286b0d1aa02e6121`,
original668-line prefix,13 links, six companions,338 Rust/governance/HEAD/index
verified. Mencius's bounded transcription check found no discrepancies.
Artifact `/tmp/mini-vpn-p3-fixture-contract-SLDwsd/` holds entry/audit-draft
and source-evidence.md, including the local standard-library fingerprint.
Final same-Heisenberg consistency review: P0/P1/P2=0/0/0, Approve only document
consistency. Six changed files and unchanged ERRORS, seven fingerprints,
32 changed-document links plus10 frozen ERRORS links all verified; inverse
recovery matches the approved763-line draft. Historical result prefix,
338 Rust/governance/HEAD/index remain unchanged. Reviewer permits only this
factual joint/result transcription and immutable `audit-final.json` without
recursive review. Rust names/paths/signatures, characterized preparation and
full joint cold/binding/ports/Layout still require their accepted freeze;
no tests, Cargo or Rust writes ran here.

## 22. Proposed canonical geometry Interface, without domain authority

Use the selected inward owner_ready Module for ReadyGeometry and
ReadyGeometryError. Keep their declarations and operations restricted to
crate::owned_upstream; all fields remain private. This is the finite value
Interface for the preparatory refactor, not a grant to create another domain:

```rust
ReadyGeometry::checked(NonZeroUsize)
    -> Result<ReadyGeometry, ReadyGeometryError>;
ReadyGeometry::session_capacity(&self) -> usize;
ReadyGeometry::process_cells(&self) -> usize;
ReadyGeometry::class_capacities(&self) -> [usize; 4];
ReadyGeometryError::ProcessCapacityOverflow;
```

The constructor stores the checked N, checked 4*N+1 and checked capacity array
[N+1,N,N,N]. Evaluate 4*N+1 before N+1, with checked multiplication/addition;
no saturation, default or arbitrary class-array input. The three read-only
operations return only those immutable values. They expose no slot, cursor,
publisher, gate, source, session identity or reservation. No Clone/Copy API is
needed for this cut: the outer plan owns the value, metadata borrows it during
allocation. Debug remains diagnostic, not a capacity/provenance certificate.

ReadyProcessPlan/Error and ReadyClass remain outer. Replace the plan's three
capacity fields with one geometry field; retain budgets, total budget, class
turn bounds and quantum bounds there. Preserve its checked entry's exact order:

1. Convert input N with NonZeroUsize::new; None returns ZeroSessions.
2. Validate class budgets in existing Settlement/Bootstrap/TargetOpen/Session
   order, preserving each ZeroClassBudget identity.
3. Call ReadyGeometry::checked; map its one arithmetic error exactly to the
   existing ReadyProcessPlanError::ProcessCapacityOverflow.
4. Checked-sum the original budgets, preserving TotalBudgetOverflow.
5. Compute each class's T and then Q in the existing order, preserving
   ServiceBoundOverflow { class }. Budget clamping/defaults remain forbidden.

ReadyIndex::new changes only its plan parameter to &ReadyGeometry, retaining
the existing initial-mask slice and result type. It reads N and class counts
from that borrowed value, never imports ReadyProcessPlan/Error or ReadyClass.
The sole current caller passes its actual plan.geometry. There is no new
caller, domain constructor or Controller installation in this preparation.
Other outer uses of N read the same canonical value. Only two existing
process-plan tests contain four direct capacity-field reads to migrate.

The two dominated foreign Layout checks from section15 are removed separately
as already selected; all actual inner mask/count/slot/tree/Layout validation,
allocation order and existing constructor failure payloads remain unchanged.
In particular, do not pull N*SOURCE_COUNT or N*2 into this geometry constructor.
Before refactoring, add the real outer-plan characterization N=usize::MAX/4,
budgets=[usize::MAX,1,1,1]: it must return TotalBudgetOverflow, without allocation.
N*5 overflow cannot preempt it. Keep all nine existing plan cases and their
error/value assertions. This is a green-to-green preparation after the full
joint/Rust freeze, not a new capacity behavior or physical backing proof.

## 23. Actual checked-constructor migration inventory

The source audit finds one new_checked definition on
OwnerSessionControllerCore<T, CheckedOwnerControllerCapacity>, one checked
OwnerSessionController alias, and18 invocation expressions in14 functions
across six files. Every invocation is currently on a cfg(test) path; no
renamed constructor/function alias or additional direct Core call was found.
The distinct LegacyOwnerSessionController/new_for_test cannot become a bound
checked caller. These are current sources, not a new production caller claim.

| File / enclosing function | Invocation lines | State at this exact constructor boundary |
|---|---|---|
| [capacity.rs](../../src/owned_upstream/owner_session_controller/tests/capacity.rs): owner_capacity_rejects_replay_storage_drift_and_returns_every_constructor_authority | 148,169 | Empty Executor; reject wrong plan, then retry returned authority with correct plan. |
| Same: owner_capacity_rejects_equal_config_horizon_collision | 202 | Empty; scalar config equality cannot replace full replay provenance. |
| Same: owner_capacity_witness_retains_exact_source_factory_primitive_layout | 251,258 | Two empty Executors with different FlowPortConfig. |
| [continuation tests](../../src/owned_upstream/quinn_leg_composition/owner/continuation/tests/mod.rs): checked_root | 112 | Empty Executor, then output-root construction. |
| Same: checked_root_with_pending_recovery | 229 | Replacement installed, attach barrier active, acceptance enqueued; recovery carrier still held by caller. |
| [physical attach](../../src/owned_upstream/quinn_leg_actor/tests/r7_physical_attach_permit.rs): compose_registered_owner_b | 77 | Real live Target flow already established; B actor supplies its actual clock. |
| [physical recovery](../../src/owned_upstream/quinn_leg_actor/tests/r7_owner_physical_recovery.rs): legacy_active_b_terminal_fixture | 69 | establish_targets=false; unresolved client OPEN, no live Owner Target yet. |
| [initial acceptance](../../src/owned_upstream/quinn_leg_actor/tests/r7_initial_acceptance_settlement.rs): same_scalar_wrong_physical_actor_and_queue_return_exact_initial_handoff | 204 | Fresh initial Executor; foreign-actor rejection is later. |
| Same: terminal_barrier_before_and_after_delivery_returns_exact_handoff_ownership | 462,468 | Initial acceptance pending/delivered; terminal tests occur after construction. |
| Same: initial_physical_root_adopts_the_prebuilt_controller_before_bounded_service | 632 | Initial Executor, then initial composition. |
| Same: accepted_initial_root_parts_transfer_the_current_actor_once | 714 | Initial acceptance not physically delivered yet. |
| Same: initial_terminal_requires_settlement_and_installs_one_legless_high_water | 842,868 | Fresh Executors; settlement/terminal differences tested after construction. |
| Same: accepted_current_actor_terminalizes_and_joins_before_legless_transfer | 1026 | Initial Executor, then initial composition/accepted parts. |
| [CurrentOnly fixture](../../src/owned_upstream/quinn_leg_composition/owner/physical_session_root/tests/current_only.rs): settled_current_only_fixture | 345 | Initial Executor, then initial composition/settled seed/CurrentOnly. |

The live fixture actually completes OPEN/Target/output/OpenResult and asserts
opened_flows=1, live_flows=1, duplicate_opens=0 before Controller construction.
TargetIoColdInventory::is_quiescent at target.rs251 requires live_flows==0;
adding that predicate to all checked inputs would reject this valid caller.
Cold exclusivity is not cold-zero. Preserve the actual live Flow/Driver/Target
without rebuilding it, minting another factory or polling it during binding.

Recovery crosses two different boundaries. At continuation test229 the
Executor has attach_barrier_pending and the caller owns EnqueuedOwnerTargetAttach.
Only lines231–234 transfer that carrier via begin_attached_recovery; line235
therefore gives output-root construction an already-recovering Controller.
Do not claim the Controller owned that cursor before new_checked, nor assume
an empty output-root input from an initially empty Controller recovery field.

None of these18 invocations supplies a pre-failed/pre-expired Executor. That
absence is not an implemented exclusion: existing capacity derive does not
validate session phase, and new_checked initializes Active without such a
check. Section6's expired-without-original-fact rejection is still a selected
future rule, not current behavior or permission to silently exclude live/recovery.

## 24. Exact return seams and unresolved same-domain binding

OwnerSessionControllerBuildFailure has one definition and one construction
site. Its existing into_parts is (clock, executor, plan, Capacity(source));
the capacity test at159 is the sole external tuple destructuring found and
at169 reuses returned clock/Executor with the corrected plan. Preserve this
capacity cause and those authorities when adding the selected fourth input;
every new modeled precommit rejection must also return that same reservation.
The earlier constructor direction remains controlling; this audit does not
invent the reservation representation or settle ordering among new errors.

Output-root new at continuation192 returns the complete Controller on failure.
Its four current callers are continuation tests113/235, test-only physical_root32,
and physical_session_root493. The B adapter returns (standby, Controller);
CurrentOnly reconstructs the original actor/handoff/Controller seed with the
exact output error. Their typed return paths do not extract an unbound Executor.
The recovery test's map_err(...into_parts().1) discards Controller on its failing
test path; that is not a production disposal/settlement pattern to copy.

Existing immutable capacity/config/replay witnesses and snapshots cannot mint
the missing domain correlation. SourceFactory capacity is explicitly not
factory identity; Executor's identity Arc stays private. SessionSnapshot has no
SessionId. Equal generation/capacity/clock does not prove a pair or reservation.
Do not add a raw identity getter or borrow the client-only gate to work around
this. A closed actual Owner cold preparation/installation operation must prove
the correlation and retain every typed return, at its existing Authority.

The remaining coupled choice is the concrete non-Clone reservation lifecycle:
actual process-domain/slot plus checked M_ctl/M_data/O/F storage, actual Owner
binding and one inbox/sender pair; expected rejection versus already-faulted
domain, duplicate binding, interruption and final publication must be explicit.
It must preserve existing live/recovery inputs and never treat returned metadata
as proof of physical custody after unknown destruction. Real parent shutdown/
adoption remain separately scoped; neither this value geometry nor a test caller
creates that parent. Full masks/charging/Layout and binding Rust scope stay open.

Mencius supplied the18-call/return inventory; Main checked the actual capacity,
constructor, live/recovery, output-return and geometry bodies. This is source
evidence plus a geometry Interface selection. Same Heisenberg reviewed919-line
SHA `13f9745018d319a360a2380332c7ea68922e476d75b1803e918a6aa29bbe24d1`:
P0/P1/P2=0/0/0, Approve only ReadyGeometry value Interface and cold inventory/
return facts; Module/Clean/System/DDIA9/9/8/8, no necessary structural
optimization. Original776-line prefix,19 links, six companions,338 Rust/
governance/HEAD/index verified. No construction-safety or full joint claim.
Artifact `/tmp/mini-vpn-p3-cold-geometry-pCRcse/` contains entry, audit-draft,
audit-identity-wording and caller-evidence.md. Final same-Reviewer consistency
review is0/0/0 Approve: seven fingerprints, six changed documents,38 changed
links/48 total links and338 Rust/governance/HEAD/index verified. ERRORS stayed
unchanged; inverse recovery matches reviewed919 lines. Only this factual
transcription followed; immutable audit-final seals the packet.
Reservation lifecycle/correlation/installation/Layout remain open;
no Rust, tests/Cargo, governance, external operation or commit/push occurred.

## 25. Installed Owner correlation, without remint or identity export

This selects the cold validation responsibility, not a ready-slot lifecycle
or a Rust grant. The actual birth graph is Supervisor1063–1129 minting the
sole factory and storing its origin/admission, followed by Executor2130–2211
retaining that same Supervisor/factory pair. Executor accepts source config,
not an injected factory. TcpFactoryOrigin::mint and factory new_bound have
only this actual production birth call. The already-live/recovery Executor
cannot be reminted: Supervisor first returns AlreadyMinted.

Three identities must stay distinct: reducer SessionId, source factory birth
origin/backing, and Executor identity used by recovery/physical handoffs.
FactoryFlowGate::admit at tcp257 checks SessionId/direction/monotonic authority,
not factory origin. DATA reduction at supervisor1678 compares the staging
origin and quarantines/poisons on mismatch; Control has no such check. Neither
operation is a side-effect-free cold validator. The cfg(test) factory fork
at tcp712 deliberately preserves origin while rebuilding gate/ledgers/progress;
ordinary Clone at597 instead shares that backing. Equal origin alone would
therefore miss that test corruption. It is not a production injection path.

Select one closed borrowed validation chain inside the checked construction
transaction; do not mint a reusable proof token, expose a raw SessionId/origin
getter, borrow the client-only gate, or accept an externally supplied factory:

1. Executor's cold preparation owns exclusive access to its actual installed
   fields and the incoming reservation. It alone invokes an Owner-only
   Supervisor validation operation, authorized by one private-constructor
   cold-preparation gate at the Executor Authority. This gate owns no resource
   or lifecycle state and authorizes no reduction, source poll or publication.
   Reusing the postcommit-observation gate for this different purpose is not
   selected. Exact names and Rust placement follow the complete joint freeze.
2. Supervisor validates Owner role, its healthy/non-expired state and the
   existing Minted(origin, admission) relationship. Active and Legless are
   legal phases; do not impose pristine, zero-live-Flow, empty replay or an
   absent attach barrier. Expired has no original fact input in this cold
   constructor and is rejected under section6. Closed/Unminted admission is
   a precise precommit rejection, never a remint or reconstructed admission.
3. Supervisor checks the reservation's declared session against its private
   model.session_id() through an inward reservation comparison operation.
   That operation only returns match/error; it does not return the actual ID,
   mint binding, change a slot, or let a successful scalar comparison replace
   the same-domain check. Ready metadata does not import Supervisor/Executor;
   Supervisor may use the inward metadata comparison. No callback is added.
4. In the existing TCP Authority, validate the borrowed actual factory against
   Supervisor's borrowed origin and sole admission: origin Arc identity must
   match, admission.flow_gate and factory.flow_gate must be Arc::ptr_eq, then
   the actual gate must be unpoisoned, open and bound to that same SessionId
   and TargetToClient direction. Compare both identities before locking the
   one gate. Do not recover poison with into_inner, close admission, increment
   highest_authority_id, allocate, notify or create a Flow during validation.
   Return the exact reason; no underlying Arc/seal/guard escapes TCP.
5. Keep success inside the exclusive preparation interval until inward pair
   installation; no detached boolean/certificate may later authorize a
   different Executor. Recovery/physical handoffs keep their existing Weak
   Executor pointer, transport seal, physical binding and FIFO checks. This
   cold source correlation does not replace or weaken them.

The checked constructor's existing Capacity error precedence stays first.
Within this new identity layer, test Role, supervisor poison, Expired, missing
origin/admission, reservation session mismatch, factory origin mismatch,
admission-backing mismatch, gate poison/closed/binding mismatch in that order.
Precise Rust error variants and ordering against the separate reservation/
storage errors remain part of the full constructor freeze. A failure returns
the original Executor/clock/plan/reservation under section24, with no source
or reservation mutation by this validation. Existing Target capacity queries
are not newly claimed callback-free or recoverable from arbitrary panic.

The gate-pointer test independently detects the existing same-origin test fork.
It does not compare each semaphore/progress allocation with an independent
Supervisor copy: none exists. Complete backing provenance additionally rests
on the closed new_bound/new_internal/Clone construction and private installed
Executor field. Do not add duplicate ledger Arcs to Supervisor for this check.
A new factory injection/backing mutation route would invalidate that source
proof and requires a new review, not a stronger claim about Arc identity.

Required future focused evidence is finite: original pair accepted; same
SessionId foreign origin rejected; existing same-origin independent-backing
fork rejected; unbound/closed/poisoned/wrong-direction cases exact-rejected;
wrong reservation SessionId returns the unchanged inputs; live Target and
caller-held recovery fixtures stay valid; original capacity error wins when
both old capacity and identity are invalid. Mutation fixtures stay inside the
owning private tests; no production getter or factory injection is introduced.
None of these new checks or tracers was implemented or run in this stage.

## 26. Actual cold-slot and pair boundaries that remain to select

The additional source audit does not find an existing cold reservation to
reuse. ReadyIndex::new at scheduler417 allocates all N slots and four trees
at once; its sole caller remains outer from_commands629. Both ReadyKey
generations are fixed1, with no production increment/reuse. The key carries
no SessionId/domain; a publisher's domain comes from its Weak<ReadyShared>.
capacity_waker at502 allocates a publisher but does not validate/reserve a
slot. claim at527 admits already-Published runtime work; it cannot act as
cold reservation. quarantine preserves backing/key and clears runnable bits,
not the lease. ReadyClaim Drop performs no settlement. These are source facts,
not authorization to treat Dormant, quarantined or dropped work as Free.

SessionOwnerMailbox::bounded at29 creates the actual two channels/progress
and empty prefetch after four configuration checks; it has no modeled
allocation-error return and does not notify during construction. Ingress,
inbox and observer have no session/domain binding. observe_capacity at84
CAS-admits one observer before allocating its pinned cache; actual waiter
registration happens on poll. The observer is not channel or ready capacity.
Dropping the inbox closes/notifies then destroys retained records; dropping
the last sender can wake a receiver (Tokio1.52.0 chan225). Neither is a clean
settlement. Current checked Controller owns none of this pair or a ready lease.

The next lifecycle selection must therefore provide actual same-Shared slot
reservation, unique binding and owning return/disposition, not a copied key,
separate gate or observer lease. Distinguish empty unpublished preparation
(no polled/escaped endpoints or callbacks) from installed/cancelled/unknown
custody. Derive admission ordering against the same ReadyFailureGate and
retirement/reuse from the actual fields and callbacks; do not guess that
dropping a future reservation restores a slot or returns its backing.
Single-pair installation, generations, initial legal masks, real source
registration and Layout still need one coupled transaction. Source validation
in section25 is one prerequisite, not that transaction or physical adoption.

Mencius provided two independent bounded source audits; Main read the actual
mint/install, gate, DATA, handoff, ready, mailbox and Tokio Drop bodies.
Artifact `/tmp/mini-vpn-p3-owner-correlation-gG66iN/entry.json` starts from the
prior final packet and freezes1156 files/338 Rust/HEAD/index. Same Heisenberg
reviewed1056-line SHA
`8fcda1a29e5f0a6213030a0755077af5c639f6c56e46f8a53df9a2cf27713cbf`:
P0/P1/P2=0/0/0, Approve only sections25–26 selection/source facts;
Module/Clean/System/DDIA9/9/8/8, no necessary optimization. The original
931-line prefix, six companions,19 links and338 Rust/governance/HEAD/index
were verified. Borrow/private access is source-feasible, not compiled; cold
reservation/install/reclaim and the complete constructor remain open.
Final same-Reviewer consistency review is0/0/0 Approve: seven fingerprints,
six changed documents,38 changed links/48 total, original931/2683 joint/result
prefixes and338 Rust/governance/HEAD/index verified. Inverse recovery matches
reviewed1056 lines; ERRORS unchanged. Only factual joint/result transcription
followed, sealed by immutable audit-final. No Rust, Cargo/tests, governance,
external gate or commit/push; full joint/P6/P3 open.

## 27. One ready-slot ledger, registration lease versus runtime claim

Select lifecycle depth in the complete inward ReadyIndex/ReadyShared Module,
not a second allocator beside it. The N metadata slots correspond to the
future N parent positions; they never contain bootstrap, actor, command,
Target or whole-session payloads. Shared metadata may use its existing Arc
allocation; a non-Clone reservation/custody handle is the exclusive right to
change one recorded occupancy, not a second counted lease or shared physical
Authority. Domain identity is the actual ReadyShared allocation, not N, a
numeric key or a separately created ReadyFailureGate.

Clarify the accepted ready terminology before adding a phase:

- A registration lease reserves a stable cell/backing position for an owning
  source/class. Once active, Idle, Published, Running, Clearing, waiting and
  quarantined/retiring registrations still charge that same Active position.
- A ReadyClaim is the one admitted visit borrowing that registration's work
  authority. It is not another capacity allocation. Pending ends the visit
  and may clear runnable bits; it does not free the registration.
- Dormant/Free has no registration lease. Reserved has a destination lease
  but no active source/claim. No source may publish into Reserved. Quarantine
  retains whether the original charge was Reserved or Active; it never
  normalizes either charge to Free.

Thus for each class c, Free_c + Reserved_c + Active_c = C_c, with retained
quarantine included in its original column; runnable claims are not added
again. C is the original [N+1,N,N,N]. Each session position has one fixed
destination in each class, and root-only Settlement has its separate extra
destination. Select reservation of the four session destinations together
when the metadata position is occupied. Activation later moves only actually
owned classes Reserved->Active; it cannot manufacture Bootstrap/Target/actor
work. Migration returns each old class lease separately and activates its
already-reserved Settlement destination. Unused class reservations stay
explicit until returned, not converted into another class or hidden as zero.
Root-only Settlement is never borrowed to admit a session.

The one slot record carries its immutable occupancy correlation plus actual
cell phases. The selected semantic alternatives are Free(next generation),
ColdReserved(correlation, class destinations), AssociatedPreparing(correlation,
identity phase, registration phases, unused bind right), Bound(correlation,
registration phases), Retiring(original correlation/remaining registrations), and
Quarantined(original nonzero occupancy). Exact Rust enum/borrow shapes are
still subject to the full freeze; no Option hole or copied counter implements
this ledger. Snapshot counts are derived from its actual fields at cold audit.
AssociatedPreparing is the explicit pre-Controller parent association, not
ColdReserved or already-bound. Bound means metadata/pair association, not P5
Live or physical adoption. Neither metadata state stores a parent payload.

| Occupancy | Unattached cancellation | Controller bind | Class charges |
|---|---|---|---|
| ColdReserved | Present, unique | Unused | All four Reserved; no registered source |
| AssociatedPreparing | Irreversibly absent | Unused until healthy identified preparation commits | Bootstrap Active only while the real parent bootstrap source exists; other classes Reserved unless actual owned preparation work activates them |
| Bound | Absent | Consumed exactly once | Actual registered classes Active, unused destinations still Reserved |
| Retiring / Quarantined | Absent | Closed, never retried as unbound | Original remaining Reserved/Active charges retained |

Association consumes the cold cancellation right but preserves the unused
bind right, key and all four charges. AssociatedPreparing never transitions
back to ColdReserved. A bind failure returns AssociatedPreparing with the same
parent association, unused bind right and actual class charges; a successful
bind consumes that right and enters Bound without acquiring another lease.
Shutdown may instead close the bind right and enter the existing migration/
settlement route; it cannot publish Bound after shutdown admission wins.

Only exclusive ReadyIndex operations allocate or return metadata occupancy.
The handle keeps that same metadata alive and carries immutable checked
M_ctl/M_data/O/F inputs; it cannot select, clear, change generations, or mint
another handle. The same handle is returned on modeled constructor rejection.
The binding transaction consumes its one installation right; no independently
callable setter permits a second Controller/pair for that occupancy.

## 28. Cold reservation, preparation and explicit unattached cancellation

The P3 identified metadata creation path selects a free position and checks declared
SessionId uniqueness across occupied records in that same domain. A declared
ID is routing input, not authenticated Owner proof; section25 validates it
against the actual installed pair later. The record's ID cannot change in
place. A corrected ID requires a legal cancellation and a new reservation.
P4 can precede authentication and must not invent an ID. Its same-allocator
AssociatedPreparing path therefore records AwaitingIdentity until the actual
authenticated candidate supplies Identified(SessionId); only that transition
checks uniqueness against other identified occupied records. AwaitingIdentity
cannot enter checked Controller bind. This is an integration constraint, not a
second allocator or a P4 implementation grant. No ID0/guessed ID represents it.
All N records, four destination arrays and checked generation/storage geometry
already exist; no side registry or unbounded reservation list is added.

For this cold operation, a single bounded pass of at most N occupancy records
can find Free and reject duplicate IDs; existing occupancy IDs are immutable
until an exclusive return. Check each actual slot guard, not an unlocked
snapshot. This N work is cold admission work, not an O(1) DATA operation or an
already-charged service unit; its eventual caller/quantum must be included in
the work freeze. A poison or corrupt record is a shared metadata fault, not
an invitation to skip that record and allocate another slot.

Before mutation, validate all new limits, destinations, actual domain/key,
generation values and the selected free record. Under its slot exclusion,
the existing strong Open->Open first-fault RMW is the last fallible admission.
Publish the recorded reservation and unique handle with plain infallible moves
after it. A fault ordered earlier rejects unchanged; a fault ordered later
does not erase the already-admitted reservation or reopen ordinary service.
No Waker/publisher, mailbox command, Target or physical session is created by
this metadata reservation itself.

Checked Controller preparation retains the original four inputs. Capacity and
section25 correlation checks precede pair installation. Prepare the one fresh
mailbox pair, sole observer, two cells, source/Flow backing and all remaining
modeled fallible storage while still unpublished. A prepared registration may
hold its allocation but must not be polled, registered in a dependency,
published or handed to a source yet. On modeled failure destroy only that fresh
unpublished preparation, then return the same reservation and original inputs.
The existing live Target/recovery/source authorities are neither drained nor
recreated. No allocator-abort or arbitrary precommit-unwind recovery is claimed.

At installation, revalidate either the exact ColdReserved occupancy or healthy
Identified AssociatedPreparing with its original unused bind right. Preserve
which branch entered and its existing class charges. Revalidate the same domain
under slot exclusion. Duplicate install, stale key, wrong domain, poisoned slot and
already-failed domain return the original inputs before mutation. After all
other fallibility, the same first-fault admission RMW orders the one bind.
Inward sender/cell and matching Controller inbox installation must then be one
infallible transaction, followed by publication of that Controller, not a
returned partially-installed Executor. The local borrow/owning aggregate,
precise error precedence and initial actual registration/eligibility table
remain mandatory full-freeze items. In particular, Bound is not permission to
select work before its actual Owner is installed; no guessed initial mask or
post-publication fallible registration is allowed by this selection.

Cold-only cancellation is distinct from every installed-session shutdown:

1. Only the original never-installed, never-published metadata reservation can
   request it, together with exclusive access to the same ReadyIndex. It must
   still be ColdReserved; all four destinations Reserved, all runnable words
   empty and no publisher/claim/dependency registration may have escaped.
2. Validate actual domain identity, exact immutable correlation, the exclusive
   cancellation right and checked next slot generation before changing fields.
   A failure retains the original reservation and all charges. A metadata
   fault/exhaustion remains at the same first-fault Authority, never reset.
3. An infallible return transaction retires the four Reserved charges and the
   unique cancellation right, clears the old occupancy, and only then publishes
   Free with the checked next generation. This returns pure metadata capacity,
   not SessionJoinedZero, parent Vacant or any physical-resource observation.
   Already-failed domain status is preserved even if known-empty metadata can
   be returned; cancellation is settlement, not ordinary service admission.
4. Slot generation changes only after that old metadata inventory is zero.
   Scheduling generation is not used to authorize reuse. Later shutdown
   migration retains the slot generation and advances scheduling generation
   once under its separately accepted contract.

Before P4 associates real bootstrap/seed/session custody, its one owning
transaction consumes the unattached cancellation right and publishes
AssociatedPreparing. A pre-auth admission directly from Free enters that
same associated alternative and never grants an unattached cancellation right.
Both paths use the same N positions and four class destinations. The real
parent retains its actual payload before any await, and activates only its
real sources. No typed metadata observation replaces that P4 ownership commit.
Identified AssociatedPreparing can enter the same Controller bind exactly once,
without taking the cold-cancel exit or recovering the consumed cancellation
right. The semantic alternatives and rights above are fixed; exact private
Rust carrier names and the fused P4 operation still require their compile shape.
Returning a checked-constructor error does not downgrade its caller's original
parent association. Bound/associated/Retiring/quarantined occupancy never
uses the cold cancellation path; its actual retirement proof remains required.

Unattached cold handles are scoped, not freely escaping owning handles. Select
an exclusive mutable ReadyIndex borrow for the cold reservation/preparation
interval, retained also by any cold-branch constructor failure. That borrow
ends only on explicit cancellation or consuming conversion to the bound/
associated owning form. Controller success owns that latter form, not the
borrow; do not propagate a lifetime through every live Controller operation.

The complete closed holder graph has only two creation contexts: unpublished
startup before any Root waiter exists, or a synchronous admission/preparation
operation inside the actually executing Root turn. ReadyIndex remains a
private Root field; no accessor or cold handle/failure can escape through the
Root Interface. The cold helper contains no await/yield/spawn/callback. Before
the Root can return or park, it must bind, associate, cancel, or retain actual
failure in an associated nonzero parent phase. An associated handle may persist
across bootstrap awaits only inside the actual parent slot, never an external
worker/result/observer. Its bind failure returns there before another await.
These holder/exit constraints are part of the Interface and must be enforced
by private call routes, the mutable borrow and source/compile-shaped tests;
a Rust borrow alone does not generally prohibit awaiting or manual polling.

An unconsumed cold handle Drop in one of those active/unpublished contexts
records bounded atomic abandoned-reservation fault and epoch dirty state in
the existing shared record, retaining its nonzero charges. No lock, allocation,
Notify, cleanup, generation advance or Free publication occurs in Drop.
Normal Root-turn finalization checks that same fault before any Pending/park or
successful observation; it returns the fatal disposition instead. Unwind
propagates from the active call with sticky nonzero state, not silent Pending.
Startup failure is observed before the Root can be published. There is thus no
legal external cold handle that can die after its Root has parked: the earlier
candidate allowed precisely that missing-wake counterexample and is rejected.
Actual out-of-turn publisher faults keep their existing safe notification path.
Discovery of any escaping cold handle or missing active-turn exit check is a
hard failure of this selection, not permission to omit wake or notify from Drop.
This is not a physical Controller/root Drop finalizer. Cold handle loss is a
metadata custody fault; ordinary Target/rejection errors retain normal returns.

## 29. Coupled acceptance and finite cold lifecycle counterexamples

P3 owns this metadata/command/ready transaction. P4 adds the actual table and
bootstrap custody; P5 consumes a parent-owned PreparingParts capability and
fuses whole-root Live publication. Neither P3 reservation nor Bound is that
parent capability. No slot reuse is permitted with associated physical or
registration inventory; deferred SessionJoinedZero/server-zero gates remain
unchanged. P3 pure-cold reuse evidence cannot satisfy a live-session retirement
or stale-callback-after-physical-reuse claim.

Future focused counterexamples must prove: N reservations exhaust exactly N
positions without extra class leases; duplicate SessionId rejection leaves
the domain unchanged; same numeric key from another ReadyShared is rejected;
old key cannot bind after successful cold cancellation/reuse; pre-fault rejects
reserve/bind while post-admission fault retains the admitted charge; old
Capacity and new identity failures return the same four inputs; explicit cold
cancellation returns exactly four Reserved destinations, including under an
already-failed domain without clearing its first cause; abandoned-reservation
Drop never frees the position and active-turn finalization cannot return Pending
past its fault; private caller/compile tests exclude an escaped cold handle or
failure across Root park; generation exhaustion retains it nonzero;
association consumes cancellation but retains exactly one bind right; rejection
preserves AssociatedPreparing; unidentified association cannot bind; second
bind and attached/bound cancellation fail without mutation; Pending/
Idle keep Active registration ownership, duplicate wake acquires no new lease,
and shutdown returns each class independently. Actual pair identities, live/
recovery preservation and the established sender/receiver/observer Drop order
must be checked, not replaced with capacity counters alone.

The selection still needs a compile-shaped constructor/return/cancellation
Interface, exact same-domain binding fields and initial Owner masks, source
registration activation and retry/expiry eligibility, charged work and actual
simultaneous Layout. Full joint review must close those together before Rust.
No production Rust or new tracer was run in this stage. Mencius supplied the
P3/P4/P5 separation and lease/claim ambiguity audit; Main checked the controlling
plan, amendment, current scheduler and mailbox bodies. The immutable entry at
`/tmp/mini-vpn-p3-slot-lifecycle-om1Kjv/entry.json` starts from the previous final
packet and freezes1156 files/338 Rust/HEAD/index. First same-Reviewer verdict on
1246-line draft was Request Changes,0/2/0: missing AssociatedPreparing transition
and an escaped cold Drop could leave a parked Root asleep. Main's in-scope
doc-only repair freezes association/cancellation/bind rights and excludes that
escape through the scoped active/startup holder graph. Same Heisenberg closure
reviewed1310-line SHA
`e8c2e42319e7378636d302d09e5e50f55b91baee32c2bd91bc54fcfee40aee41`:
P0/P1/P2=0/0/0, Approve only the revised selection; both P1s explicitly closed.
Module/Clean/System/DDIA9/9/8/8, no necessary optimization. Original1067-line
prefix, six companions,19 links and338 Rust/governance/HEAD/index verified.
Final same Heisenberg consistency review: P0/P1/P2=0/0/0, Approve only document
consistency. Seven changed fingerprints and48 local links verified; inverse
reconstruction matches reviewed1310 lines, original1067 joint/2726 result
prefixes preserved,338 Rust/governance/HEAD/index frozen. Reviewer permits only
this factual joint/result transcription and immutable audit-final sealing,
without recursive review. This is not compiled borrow/exit or runtime evidence;
actual Interface/exit proof, activation/Layout and full joint/P3/Case C/R7 remain
incomplete. No Rust, tests/Cargo, external or commit/push execution occurred.

## 30. Proposed opaque short-borrow constructor and owning return forms

This refines sections27–28 into a Rust-shaped Interface, not compiled evidence
or a grant to add types. Use the selected private default-path owner_ready
Module, with original paths rather than re-exports. The Module still owns only
ready metadata; commands, mailbox, Target and physical parent payload stay at
their already-selected Authorities. All new fields below are private.

Select an opaque struct, not a visible enum exposing a mutable ReadyIndex:

```rust
// Illustrative declarations inside owner_ready; bodies remain to implement.
pub(super) struct OwnerReadySessionReservation<'r> {
    access: &'r mut ReadyIndex,
    state: UnboundReservation,
}
enum UnboundReservation {
    Cold(ReservationCustody),
    Associated(ReservationCustody),
}
pub(super) struct OwnerReadyAssociatedReservation {
    custody: ReservationCustody,
}
pub(super) struct OwnerReadySessionBinding {
    custody: ReservationCustody,
}
```

ReservationCustody is non-Clone/non-Copy and contains the same strong metadata
reference, immutable key/checked limits and a private cold-abandonment
disposition. It is the existing recorded occupancy's unique operation right,
not an additional lease, physical payload, root owner or second failure gate.
Both input branches use a short borrow of the actual Root-owned ReadyIndex.
Associated custody is lifetime-free at rest in its real parent; only the
synchronous checked-construction attempt wraps it in that same-domain borrow.
This strengthens confinement without putting a lifetime into a live Controller.

ReadyIndex's cold reserve operation returns the opaque Cold form. Its separate
associated-reentry operation consumes the original associated owning form,
checks its actual Shared against the borrowed index, and returns the short
Associated form or that same owning form plus the precise rejection. It does
not allocate, change registration, reopen bind/cancel rights or perform bind.
An invalid reentry has not entered new_checked; its error is not reordered
against that constructor's Capacity error. No method returns raw index, Shared,
key, origin, slot guard or a generic callback. There is no Deref/DerefMut.

The checked signature and existing failure Interface become:

```rust
pub(super) fn new_checked<'r>(
    executor: OwnerTargetExecutor<T>,
    clock: SessionMonotonicClock,
    plan: OwnerSessionCapacityPlan,
    ready: OwnerReadySessionReservation<'r>,
) -> Result<Self, OwnerSessionControllerBuildFailure<'r, T>>;

// Failure fields: original clock, executor, plan, ready, plus precise source.
pub(super) fn into_parts(self) -> (
    SessionMonotonicClock,
    OwnerTargetExecutor<T>,
    OwnerSessionCapacityPlan,
    OwnerReadySessionReservation<'r>,
    OwnerSessionControllerBuildError,
);
```

These functions live in Controller, not owner_ready. Narrow new_checked,
BuildFailure, BuildError and its source/into_parts methods together to the
owned_upstream scope; keep their current Controller alias/output operations
otherwise unchanged. At their respective direct-child Modules, pub(super)
names that scope. All18 actual constructor calls and every BuildError/Failure
use are inside it; source audit found no wider caller. Do not pair a narrower
input with an unchanged pub(crate) signature and suppress private_interfaces.
No pub use, new facade export or public RawReady type is selected.

The failure struct, wrapper and UnboundReservation enum themselves do not
implement Drop. Only the contained ReservationCustody handles abandonment.
It records fault+epoch dirty only while its private disposition is armed cold;
associated, bound and explicitly-cancelled dispositions do not run cold cleanup
or return any capacity. Their underlying registration inventory remains until
real settlement. This is not the deferred physical Controller/root finalizer.

Consuming the non-Drop wrapper can move its access/state fields normally.
The inner Drop-bearing custody is always moved whole, never destructured into
its fields. At the final successful bind or association transition, disarm its
cold-abandonment disposition and move that same object into the corresponding
lifetime-free owning wrapper. Do not use Option::take, unsafe, ManuallyDrop,
mem::forget, Arc duplication of an operation right, or a replacement token.
The mirrored Drop disposition must agree with the record's rights at each
completed transition; only the owning transaction may change either. The
record, not the Drop disposition, supplies capacity/registration observations.

The checked Controller result cannot store OwnerReadySessionReservation<'r>,
even when its current variant is Associated. The final inward installation
consumes only OwnerReadySessionBinding, which has no ReadyIndex borrow.
Its real strong Shared reference keeps metadata alive, not the Root, Target,
actor, command or notification-driving task. No lifetime-free type by itself
proves root liveness. Actual pair/source installation remains required below.

## 31. Explicit failure exits and one final installation edge

Cold-only cancel consumes the opaque input and returns either success or an
owning failure containing that exact input and cause. It uses its own stored
exclusive index access; no second index argument can accidentally alias it.
Under slot exclusion, compare the actual Shared, key, ColdReserved rights,
four Reserved charges/no escaped sources and checked next generation. Retain
the original on any rejection, including NotUnattached for Associated.
On success, disarm the contained cold Drop, retire the four charges and publish
Free(next generation) through the infallible transaction in section28. Drop
the consumed custody only after releasing slot exclusion. The still-borrowed
ReadyIndex owns Shared backing, so that Drop cannot be its final strong owner.

An Associated input has a separate consuming return operation:
`into_associated(self) -> Result<OwnerReadyAssociatedReservation, Self>`.
It projects only the already-associated variant, ending the short index borrow;
Cold returns the same input unchanged. It does not lock, mutate metadata,
clear failure, allocate or notify. This is return to the same parent phase,
not a rollback to ColdReserved. These two exits, not an optional payload or
generic extraction callback, exhaust recoverable construction failure handling.

### Terminal constructor failure has a total owning exit

Generation exhaustion or metadata fault can make cancel reject while bind is
also forbidden. The terminal exit belongs to the cancellation failure itself:
`into_constructor_quarantine(self) -> OwnerReadyConstructorQuarantine`.
It exhaustively consumes either actual private UnboundReservation variant,
ending the index borrow and retaining the original tag, complete custody and
precise cancel cause. The opaque, non-Clone/non-Copy lifetime-free result has
no bind, associate, cancel, raw-index or reusable-reservation conversion.
Normal Associated rejection still uses its original into_associated return;
this explicit terminal exit must not replace that normal parent-return path.

This consuming operation first records the actual first fault and atomically
marks epoch dirty in the same Shared, retaining an earlier cause unchanged.
It needs no slot lock, allocation, callback or successful gate admission. A
poisoned slot is not recovered or overwritten: the original recorded phase
and four charges remain, globally barred by the monotonic failed gate. It
then disarms abandonment only for Cold and moves the complete tagged custody
into the frozen owning form. Associated keeps its actual tag and disposition.
This is permanent quarantine, not a relabeling, generation change, physical
settlement or Free.
No snapshot/token replaces the original metadata custody.

The 2026-09-09 A1 audit exposed why the prior partial Cold-only Result was
insufficient: its Associated Err still borrowed the index, and the opposite
projection could return another borrowed Cold Err. Same Reviewer
01a0864d-176a-79d0-9aee-41db8a032ef4 approves this total same-Module correction
0/0/0 under standing A1–A4 auto-repair authority. It changes no constructor
input/failure tuple, normal Associated return, resource ledger or write scope.
Both actual tags must be tested for borrow termination, unchanged charges and
separate first-fault/exact-cancel-cause retention; code/runtime proof is still required.

The actual outer holder is the existing Root, not a fictitious P4 parent.
Select one inline ConstructorQuarantined arm in its single execution-state field,
beside Idle and Running. That arm owns an OwnerConstructorQuarantine<T> containing
the original Executor, clock, session plan, exact constructor cause and the
OwnerReadyConstructorQuarantine including its cancel cause. Refine the internal Root
and execution-state types with the actual static Target type T for this arm;
no dyn Target, boxed callback, optional slot or second failure collection.
This is the one current synchronous construction failure, not N bootstrap/
session slots and not an owner for actors or physical CurrentOnly roots.

Before cold reserve/construction, require the execution state Idle and the
inline failure destination available. It cannot overwrite Running or an
earlier cold failure. On cancel failure, the caller moves the original inputs
and frozen custody into that arm using only infallible field moves, before
returning Root-fatal. At most one such entry exists: the gate is permanently
failed before it is published, so no further construction/ordinary admission
can consume another destination. No extra class lease, task or allocation is
needed; the eventual Layout accounts for the concrete enum arm and original
coexisting inputs, not zero-size metadata or a newly allocated copy of them.

For unpublished startup, deepen the existing build-failure result with a
distinct owning ColdPreparation failure arm. It returns the actual prepared
ReadyIndex, this complete OwnerConstructorQuarantine<T>, and all other original
process-plan/command inputs or prepared owning fields that still exist at that
point. No borrow survives, and no self-reference to that ReadyIndex is stored.
Early failures before metadata exists keep their existing exact-return arm.
An active Root retains its own ReadyIndex field and the same failure entry;
an unpublished builder instead returns both together. These are concrete
Root/build-failure ownership choices, not reliance on a future P4 table.

Observation of either terminal result borrows the retained values; there is no
extractor that reopens cold construction, drops the Executor to report success,
or makes the domain reusable. Original Target/recovery state is not drained
or reconstructed. This closes modeled cancellation failure storage/borrow
termination, not arbitrary Target-query unwind recovery or eventual physical
shutdown. Existing Root/source regression fixtures must select a real static
test Target type when that internal type migration is later authorized; no
dummy Target value or new production adapter is created to satisfy inference.

Inside new_checked preserve: existing Capacity derivation first; section25
Owner/source correlation next; then ready limits/phase/domain and actual fresh
storage/pair preparation; final under-slot revalidation and shared admission
RMW last. Identity errors cannot be hidden by a new early phase/gate check
inside that constructor. Cold-reserve/reentry errors precede entry because no
constructor input exists yet; they are not claimed to have this precedence.
Full error variants and fresh-allocation ordering still need the Layout row.

The reservation's final bind is reachable only from this checked preparation,
not from a live Controller or independent post-publication setter. Before it,
the complete original Executor/clock/plan/reservation remain locally owned and
all fresh mailbox/cells/observer/source backing is unpublished. After the final
RMW, move the same metadata custody, inward SessionCommands and actual paired
Controller inbox without allocation, registration, callback, validation or
fallible operation, then publish Controller. No fallible step is postponed
until a later setter or publication. Metadata Bound still cannot be selected
until actual Owner/source registration is installed; initial masks/activation
must be preflighted as part of this same final edge, not invented here.

Keep checked-only inbox/binding ownership in the checked state/Executor branch,
using the existing checked-versus-legacy type distinction. Do not add an Option
ready/inbox field to every Core<T,C>, initialize legacy with empty production
authority, or let unbound service bypass a checked binding. The final branch
fields and one actual pair are joint-freeze obligations, not solved by these
type sketches. A later output-root construction error still returns the whole
already-bound Controller, never this unbound four-input failure.

Error Debug stays manually redacted and does not add T: Debug or reservation
Debug bounds; the Ready handle needs no diagnostic field exposure. The current
capacity test's into_parts at159/retry at169 must use its returned reservation,
not reserve again. Err(_) at202 and temporary Controller Drops at251/258 need
explicit lifecycle handling in their fixtures. All six caller files must keep
a real same-domain owner for as long as their successful Controllers live;
returning a lifetime-free Controller from a helper is not proof that an
ephemeral ReadyIndex or its Root may be discarded. Live Target and caller-held
recovery remain valid; no pristine or zero-inventory shortcut is selected.

## 32. Root exit topology and the synchronous cold interval

Current source, not future readiness: Root stores its only ReadyIndex at550;
from_commands629 is the only creation call. Notification Arc/cache at630–631
share the metadata Notify but do not own ReadyIndex. Actual poll callers are
only five private test leaves. Neither current file has a cold handle, scope
finalizer or registration-return operation. Therefore this is a required
integration topology, not a claim those operations already exist.

Mencius and Main checked all15 explicit poll_ready_unit exits:

| Current source lines | Existing disposition to preserve |
|---|---|
| 703 | Existing process fault, unfinished claim/custody retained. |
| 709 | Source-unwind recovery; clear in-flight only if actual quarantine succeeds. |
| 720/727/737 | Select/resident/claim failure, no new admitted work. |
| 745/765 | Invalid admitted command custody, actual resident quarantine. |
| 788/792 | Missing claim/finish failure; retain real source outcome and remaining claim. |
| 795 | Completed admitted Visited, including actual MailboxOwned or internal Waiting. |
| 803/814/819 | Rescan, possibly with a newly recorded fault. |
| 806 | Park epoch failure, Quarantined. |
| 817 | Sole outer Pending after registration, summary/epoch/gate checks. |

A later unrelated publisher fault cannot revoke an already-admitted command:
scheduler527/570 orders admission and permits its actual finish. Retain the
existing Visited result in that case; do not turn a real MailboxOwned into an
unperformed/quarantined command or retry it. Section28's cold successful-exit
check is not a license to rewrite that accepted source result.

Select disjoint synchronous cold preparation and ordinary claimed-source
phases. A Root invocation completes any cold scope before beginning an ordinary
source claim, and never starts cold construction after such a claim. A cold
failure must be observed before the next ordinary admission. Cold-only helper
results carry no borrowed reservation or cancellation failure through a Root
return, Pending, cached waiter, task or external observer. All temporaries and
their destruction end before its caller performs the final fault/epoch/park
checks, not after the last park check. Associated return goes back into its
same real parent position before another await; no metadata wrapper stands in
for that still-separately-scoped physical parent.

The existing poll body has early returns, so a future implementation must
converge cold exits inside that synchronous scope, then run one Root-owned
finalization path for any cold outcome before continuation/park/publication.
Use concrete internal control flow, not a user closure, optional Driver or
generic finalization callback. A completed existing source unit still reports
its actual result as above; it admits no new work after the fault. A cold
abandonment cannot yield Pending, Rescan-as-healthy or new service. Neither
catch_unwind nor ReadyClaim Drop implements this finalization. On unwind the
cold custody Drop marks the same first-fault gate and epoch dirty; the next
living-root entry observes it before source-unwind recovery, as today.
The new ConstructorQuarantined execution arm is never treated as an unfinished
ReadyClaim. Root entry returns fatal while preserving its actual failure entry;
it does not run resident quarantine, clear in-flight to Idle, or retry bind.

Unpublished startup has no armed Root waiter. Its concrete constructor must
settle successful cancel or retain the actual failed preparation before it
can publish a Root; no source/observer can receive a cold handle. Active
construction likewise cannot be made reachable by adding an accessor for
ReadyIndex. The permitted contexts are full private caller graphs, not merely
functions returning a non-Future type. Safe Rust can forget values or await
while borrowing; those operations/escapes must be absent from the closed graph.
No leak, synthetic vacancy or unchecked parent association is a fallback.

The no-callback cold rule excludes added scheduling/worker/user-supplied
construction callbacks. Existing fixed Target capacity-query calls remain
explicit per section25; they are not newly proved pure or panic-recoverable.
No dependency/source poll, external registration or Waker publication is
allowed during fresh preparation. A panicking query can unwind with sticky
metadata fault, but exact rollback of arbitrary Target panic/destruction is
not claimed. Root/physical-payload destruction remains the separate Case C
obligation, not a reason to invent cold Drop cleanup or Notify under guards.

## 33. Interface falsifiers and remaining coupled freeze

Future compile/source checks must reject raw ReadyIndex/enum-field exposure,
the old wider failure signature, moving fields out of a Drop type, a Controller
containing 'r, leaked cold failures across Root return, and bound-state fallback
to legacy. Future runtime tracers must preserve exact cold/associated rejection
inputs and repeated use of the same handle. Disarm only at actual cancel,
association, bind or the selected terminal cold-quarantine conversion; that
last exit must first commit the same first-fault/epoch dirty state and retain
the original whole custody and four charges. Retain four charges on Drop/
cancel failure, and reject wrong-Shared associated
reentry. No second slot/pair may appear on retry. Real live/recovery fixtures,
sender/receiver/observer destruction ordering, fault-before-park and the
already-admitted MailboxOwned trace must all survive through their actual paths.
Include constructor rejection followed by real cancel generation exhaustion
and slot poison: both must end the borrow into the actual active-Root or
unpublished-build owning failure, retain the same Executor/cold custody and
both precise causes, keep four charges, and reject another ordinary admission.
Normal Associated cancellation rejection retains its original associated return.
Explicit terminal conversion may consume either original tag, but cannot
relabel it, return its charges or expose a reusable exit. No test substitutes a P4 parent.

Selection trade-off: one opaque short-borrow input, a lifetime-free owning
output and explicit associated reentry/return avoid raw-index access, generic
typestate parameters on Controller, optional production binding and unsafe
Drop extraction. The cost is a cold-only borrow transition at the real parent;
it adds no DATA indirection, allocator, dynamic dispatch or capacity pool.
Physical Layout still must count actual wrappers/backing, not only N or Arc size.

No new Rust, compiler probe, tests/Cargo/fmt/governance or external operation
was run. The previous packet and1156 files/338 Rust/HEAD/index are frozen by
`/tmp/mini-vpn-p3-binding-interface-Ec0aMh/entry.json`. First same-Reviewer review
of1575-line draft returned0/1/0 Request Changes: cancel failure retained a short
borrow with no terminal owning exit. The causal doc-only repair above selects
the original frozen custody and complete input retention at the actual Root/
unpublished build failure; no P4 parent is assumed. Re-review closed that P1
but found one P2: the disarm falsifier still omitted association/terminal
quarantine. Its wording is now synchronized with the selected transitions.
Same Heisenberg final selection review on1654-line SHA
`74caaae1d055c0695bc8a0138365bb1423d4cf48afc74d25b66ed9700ac61859`:
P0/P1/P2=0/0/0, Approve only this architecture selection. Both findings closed;
Module/Clean/System/DDIA9/9/8/8, necessary optimization none. Reversible repair,
six companions,19 links and338 Rust/governance/HEAD/index verified. Final same
Heisenberg consistency review is0/0/0 Approve, document consistency only:
seven fingerprints/48 links, reverse recovery of1654 reviewed lines, original
1322 joint/2776 result prefixes and338 Rust/governance/HEAD/index verified.
Reviewer allowed only this factual joint/result transcription and immutable
audit-final sealing without recursive review. No semantic expansion, compiled
Interface or runtime acceptance follows.
Initial pair fields/registration/masks, original
actor-input custody/release retry, actual physical ports/expiry, contained
work and simultaneous Layout remain mandatory before full joint acceptance.
Nothing here opens P4/P5, Rust, commit/push or full-P3 completion.

## 34. Single-pair fields and the consuming installation boundary

2026-09-08 selection reviewed0/0/0. This completes the field and
installation choice from sections30–33; it does not freeze activation, owning
ports, all service Interfaces or the full joint packet. Names below describe
private storage, not a Rust allowed-files grant.

Preserve the existing checked C versus test-only Legacy C distinction. The
actual CheckedOwnerControllerCapacity gains the sole SessionOwnerInbox beside
its immutable witness; Legacy keeps neither inbox nor a dummy ready binding.
Keep Core's executor-before-capacity field order. Its existing generic methods
are not specialized by adding checked-only inherent methods; section35 closes
the required route distinction instead of relying on Rust method dispatch.

Append one private owning service state to the actual OwnerTargetExecutor:
Unbound, or Bound containing commands, Flow-service backing and the original
OwnerReadySessionBinding, in that field order. Unbound is the real predecessor
Executor, including valid live/recovery state, not a default for a checked
Controller. Every checked Controller must contain Bound; Legacy/prechecked
operations cannot install Bound or downgrade it. No Option inbox/binding in
generic Core, parallel command holder, second gate or per-Flow send cell.

The complete SessionCommands implementation moves inward under Executor:
ingress, sole capacity observer, both SendCells and their exact owning phases,
origin validation, transfer/Full/Closed/unknown disposition, selector, rearm
and quarantine. Moving only two cells and leaving transfer Authority in outer
Root is not this selection. Neutral owner_ready retains metadata only. Existing
unbound prototype characterization may exercise that same implementation via
its separately frozen private test seam; it does not supply a bound Executor's
Driver or keep a second copy of its commands. Exact Move operations, test
privacy and deletion/provenance acceptance still precede any Rust execution.

Checked construction uses one closed consuming Executor installation operation,
not a post-publication mutable setter. Only new_checked invokes it. It consumes
the original Unbound Executor and original borrowed reservation after existing
capacity preparation, validates their actual identity and allocates fresh
backing, then performs the selected final bind RMW. Modeled failure returns
that same Executor/reservation and precise cause, for the enclosing constructor
to return the same clock/plan too. A Bound input is rejected intact; no replace,
extractor or retry can obtain its existing inbox, sender or ready binding.
The eventual signature/visibility must close inside owned_upstream with that
sole call site and the sections30–33 borrowed/owning exits. It cannot introduce
a callable pair-install/rebinding port for an already-published Controller.

Inside that one operation, call the existing SessionOwnerMailbox::bounded once
with checked M_ctl/M_data. Consume its returned ingress/inbox together in one
non-Clone private preparation aggregate; never accept separately supplied
endpoints. Acquire the one capacity observer from that exact ingress, create
two genuinely Vacant cells, and prepare the actual bounded Flow-service
backing. No fabricated PeerOpenResolved, ingress clone, second observer,
dependency poll, Waker publication or Target/Driver operation occurs here.
Existing from_open_completion requires a real Held OpenCompletion and cannot
construct this empty pair by substituting a request/result or initial mask.

The pair's identity proof is its closed construction and move graph, not equal
capacity or a new copied ID. session.rs29–70 constructs both physical lanes and
their single progress object together, behind private endpoint fields. Tokio
1.52.0 bounded.rs1501 provides Sender-to-Sender same_channel; it is not a
Sender-to-Receiver comparison. Do not invent that API or claim progress-Arc
equality alone proves the physical lanes match. Later provenance must lock the
sole pair constructor, no endpoint reconstruction/replacement and no Owner
ingress clone/extraction. Existing public/Client mailbox APIs are unchanged.

Keep the temporary aggregate non-Drop, with commands before inbox, so final
success can move whole fields safely. All validation, capacity/layout/backing
preparation and the final same-domain admission RMW precede its split. After
that edge, installing Bound into the resource-free Unbound service field,
returning the matching inbox and constructing Core are only infallible owned
moves. Existing progress/witness preparation is already complete. No helper
registers sources, polls, allocates, validates or emits a callback in this gap.
The binding operation never clears/rebuilds original Target/source/recovery
state. Output-root new rejection later returns the entire already-bound
Controller, not this temporary pair or an unbound Executor.

### Destruction order is explicit, but not terminal settlement

For this selection the structural order is:

| Owner | Automatic destruction order relevant to the pair |
|---|---|
| Fresh preparation | commands, then inbox; any original constructor inputs are returned, not members destroyed with this fresh aggregate |
| SessionCommands | ingress, capacity observer, cells, selector, quarantine |
| Capacity observer | cached registration, then its exclusive lease, then epoch values |
| Bound service | commands, Flow-service backing, ready binding |
| Executor | existing identity/config/Supervisor/Target/factory/Flow/effect fields in their original order, then appended service state |
| Checked Core | entire Executor, then checked C including inbox, then existing progress/clock/phase/retained fields in their original order |

Before the temporary aggregate exists, observer acquisition failure drops only
the freshly created empty ingress/inbox locals; neither was ever polled or
published. At any modeled later preparation rejection, both cells and physical
mailbox lanes remain empty, the observer has no registered Waker, and original
Executor custody returns untouched. Keep cached-registration-before-lease
destruction even on these empty paths; CAS-before-allocation remains unchanged.

For a live bound owner, this table is not a cleanup algorithm. Dropping the last
sender can wake an already-registered receiver, and inbox Drop calls close and
Notify before destroying queued/prefetched commands. Executor's existing
Supervisor/Target/source drops can also release physical authority; merely
keeping the metadata binding later in the field list does not settle it.
Normal lifecycle must use the accepted real terminal/Flow/mailbox settlement,
retained outcome and publisher retirement before final field destruction.
Audit callback and actual outer-guard release on those operations; do not add
Drop-notify under arbitrary actor/Target locks, detach, or a fake zero result.
Unknown whole-root destruction and arbitrary Target Drop/panic remain Case C
obligations, not outcomes proved by this field selection. No new custom Drop
on Core/Executor/preparation is selected to disguise those missing operations.

## 35. Bound route isolation and the initial-registration boundary

Checked-only inbox ownership is necessary but insufficient for isolation.
Current Controller's generic impl at1509 exposes try_read_target3089,
retry_target_write3116, continuation3143, actor input3232 and recovery2783.
The same effect executor can reach old apply_target_failure4878 through
HalfCloseSink failure4597 or write failure4850. Today that helper sends RESET,
mixed-dequeues the Driver, then immediately reduces the event. Neither adding
an inbox method to checked C nor changing only read_source.rs cuts those paths.

The selected Bound owning state governs these actual shared operations:

| Path | Required Bound behavior; Unbound characterization remains separate |
|---|---|
| Old combined Target read/dequeue/reduce | Reject before Target/source reservation, polling or mutation; do not use it as scheduled source service. |
| Source read failure | Retain failed work and stage the sole RESET in its real Driver; later charged cell/mailbox/inbox service owns intake. |
| Write retry or effect Offer failure | Enter the same Flow-local failure transaction, including original Offer/segments; never call the legacy mixed RESET dequeue. |
| Effect HalfClose failure | Enter that same transaction with the original completion capability, not a second feedback implementation. |
| Actor/recovery/continuation effect entry | All use the same Bound-aware effect semantics; actual remaining effects retain their original continuation/output owner. |
| Positive sink write/half-close completion | Preserve actual observed SinkAccepted/SinkHalfClosed reduction and E-page admission; these are not Driver RESET commands. |

Shared error-producing work cannot simply reject after Target already failed:
the exact failed operation and independent pending sink ownership must first
enter the accepted Flow completion phase. InTransit feedback is not an ordinary
continuation waiting for its own excluded inbox. Current raw/legacy combined
dequeues must be unreachable from Bound, including indirect generic calls;
no overload assumption, test-only fallback, raw Driver getter or exception to
the root's ordinary/continuation/source-only owning-turn rules is permitted.
The source audit and future focused tests must enumerate those caller chains,
not only search the new scheduler for try_recv_next. Generic raw methods that
cannot honor the bound turn contract must reject before mutation; valid shared
actor/recovery/effect service must be migrated, not disabled wholesale.

### A fresh pair does not imply an empty session or ready sources

The existing live fixture opens actual Target/source state before new_checked
(actor tests3740–3757). The recovery fixture installs replacement/attach barrier
and enqueues acceptance before construction, but its recovery carrier is still
caller-owned; only begin_attached_recovery after construction moves it into
Controller (continuation tests215–239). Those are real obligations, not proof
that every pending write/join/completion/Driver queue is zero. The constructor
cannot recover their readiness by polling or discard them to obtain emptiness.

Distinguish four transitions: fresh pair allocation; metadata Bound installation;
actual per-source dependency registration/initial work; and real owning-caller
eligibility/publication. Only the first two are selected above. There must be
no Session selection before the actual owning ports/caller and legal work are
installed. A reserved class charge is neither an active source nor a P5 Live
session. Bootstrap ownership already associated with that slot stays distinct.

Initial registration must be a named, bounded work obligation against actual
retained state, not all-five-ready, [1,blocked], an invented OPEN, or an assumed
socket-ready flag. Registering a never-polled dependency, classifying an already
queued Driver item, and retrying a credit-blocked read are different work.
The cold preparation may reserve/index actual F storage without I/O, but any
F population/classification walk must be charged as cold work, not hidden in a
constant service quantum. Subsequent real changes such as recovery adoption
must retain their own work publication; a constructor snapshot cannot cover
work that the Controller did not own yet. Positive registration must poll the
real dependency; Pending must leave that retained registration, not self-ready.

The exact initial state table, per-source producer/registration/recheck ports,
activation transaction and its contained work remain open with the existing
owning-port row. No default is selected to fill them. In particular no new
fallible activation may follow the final fallible output-root construction in
fused publication, and no post-publication pair setter supplies the omission.
Do not claim these constraints are an implemented scheduler, P4 table, physical
adoption, end-to-end progress, shutdown closure or full P3.

Future falsifiers for this selection: independent/mixed endpoints or extra
Owner ingress; Bound without checked inbox or checked-with-Unbound; second
pair on retry; resource mutation before old-call rejection; read/write/half-
close/actor/recovery/continuation reaching either legacy mixed dequeue;
synthetic completion/ready mask; changed exact live/recovery return state;
fresh-observer registration or callback during construction; callback under an
outer physical guard; runtime Drop/close relabeled drained/settled/Free. The
complete command Move needs its own unchanged characterization before behavior.

Same Heisenberg selection review: P0/P1/P2=0/0/0, Approve only sections34–35;
Module/Clean/System/DDIA9/9/8/8, necessary optimization none. Reviewed1849-line
SHA `f003b5ac55defd257656cc4b82b2cb3727dafcb537995fe87c6c7bd42ec47f9d`;
original1664-line prefix, six companions,19 local links and338 Rust/governance/
HEAD/index independently verified. Artifact entry/audit-draft is under
`/tmp/mini-vpn-p3-mailbox-pair-kghwmH/`. Initial registration/activation, actual
ports, work/Layout, complete Move Interfaces and runtime lifecycle remain open.
No Rust/compile/test/Cargo/fmt/governance, external operation or commit/push.
Same Heisenberg final document-consistency verdict:0/0/0 Approve. Seven
fingerprints/48 links,338 Rust/governance/HEAD/index and inverse1849-line
selection verified; original1664 joint/2836 result prefixes preserved. Only
this factual joint/result transcription and immutable audit-final sealing
were permitted without recursive review. No semantic or implementation grant.

## 36. Initial work: internal publication, not invented receiver readiness

2026-09-08 conditional protocol reviewed0/0/0. Select Owner-local queued-
work publication for the closed Bound route. This refines section35's initial
registration obligation: a fresh internal mailbox need not first be polled to
become schedulable after its first genuine send. It does not remove actual
Target registration, the sole capacity observer/Full rearm, lane-specific
Driver/inbox polling, controlled Wakers or the existing clear/recheck protocol.
The public/Client/unbound prototype does not inherit this exclusivity claim.

The reason is the complete producer graph, not equal SessionId or capacity:
Executor owns the original factory and both source endpoints, its reservation
never leaves the synchronous read operation, and Target receives values rather
than those endpoints. The sole fresh mailbox ingress stays in Bound commands.
Future source/command receipt cannot escape the whole Controller/output owner.
Code-aware constructor/caller provenance must preserve those restrictions.

The counterexample exists in generic TCP: SinkDelivery::Drop and
HalfCloseDelivery::Drop can enqueue Control completions through retained
permits (tcp.rs2193/2253). Current OwnerTarget instead calls Target write/
half-close directly and does not use those Driver delivery operations. Factory
and mailbox ingress are Clone-capable types; private field ownership alone
does not authorize exporting either. Any future legitimate same-pair producer
or delivery guard outside the owning turn falsifies this protocol. Stop and
restore a fully proved external-registration route; never relabel a normal
external producer as fatal to preserve an exclusivity assumption.

Reuse non-owning work requests in the already-required F service backing and
two session inbox positions. A request means another bounded probe/disposition
is owed, not that a queue contains exactly one command. It owns no payload,
permit, actor, extra lease or second pending-command queue. The actual owning
phases remain Driver, cell, mailbox/prefetch, reducer/effects/completion. Use
existing revision/dirty publication and persistent cursors; do not add a count
that must equal queue length or an F-wide callback scan.

| Actual transition | Work that the same owning transaction must retain |
|---|---|
| DATA or CLOSE successfully commits to the source FIFO | That exact Flow's Ordered Driver probe is due. No Driver pull or send is fused into production. |
| The sole failure RESET successfully stages | That Flow's Control Driver probe is due; its original failed work stays in the accepted Flow phase. |
| A cell's real send returns MailboxOwned | The corresponding actual inbox lane is due, before normal turn exit. No duplicate receipt is created. |
| A selected Driver/inbox probe returns one command | First retain the actual command in its owning destination; keep a later probe due because a coalesced request can cover more than one queued item. |
| A selected probe reaches genuine idle/registration-or-publication | Resolve only that probe's request with the existing revision/recheck; never erase newer dirty work. |
| Source revocation, receiver close or a sender's normal terminal retirement | Retain the applicable lane-drain probes even if no successful send occurred. Close is a separate work edge. |

Publish these local requests inside the complete entered owning transaction,
on normal success and recoverable error exits. An inner enqueue may succeed
before an unrelated outer operation returns Err. A caller's final-Ok hook is
therefore insufficient. Reuse the selected release/notification finalization:
local metadata before exit; actual outer Notify only after every inner and
physical actor first-failure guard has been released. Retain a true admitted
MailboxOwned outcome even when another source later faults the process.

An enqueue can also succeed physically before its wake unwinds: Tokio's send
pushes before wake, while TCP CLOSE/RESET marker updates follow their send.
No successful-return-only proof covers this unknown interval. Existing
TransferringUnknown/Flow staging and entered-root quarantine keep their exact
uncertain disposition; neither a local work request nor a copied command can
repair it. Do not retry staging, report empty, free a generation, or weaken
the first-fault rule. This protocol changes normal scheduling evidence, not
the ownership semantics of unknown send or arbitrary owner destruction.

### The cold/import table

Prepare requests with fresh backing before the bind RMW, without touching
Target, Driver or inbox I/O. Any population/classification pass is explicitly
cold work bounded by the admitted F population, not hidden constant-time
service. Existing original Flow/source/recovery objects are never rebuilt.

| Actual input/work | Initial obligation, not an assertion of readiness |
|---|---|
| Fresh private mailbox, empty prefetch and both vacant cells | No inbox probe is due merely from construction. The first actual send or close supplies the work edge. |
| Existing retained source pair | One import probe is due for each of its Control and Ordered lanes. Do not assume their queues are empty from the OPEN-only fixture or a count snapshot. |
| Source absent in a retained failed Open/terminal record | No invented Driver or drained result. Its actual completion/Target/terminal phase supplies the relevant obligation. |
| Existing pending write/join/completion | Index its real retained phase for the selected category and admission rules; do not stage a second command or invoke an already-failed Target. |
| Existing real Target with demanded Read/Write/Join interest | Common-notification registration precedes the named registration/probe. A diagnostic snapshot is not an Attempt or retained waiter. |
| Recovery carrier still held by the caller | Preserve the actual Executor attach barrier; do not create Controller recovery work before it owns the carrier. begin_attached_recovery must request the work when that real transfer happens. |
| A later genuinely new source | Its queues are known fresh at their closed creation; subsequent actual production/close requests their work. External Target demand still needs real registration. |

Due is distinct from eligible. An occupied send cell, Full capacity wait,
ordinary output exclusion, retained failure/completion, unavailable actual
physical port or recovery phase can prevent a due probe from running. Keep
its obligation without continuously publishing a runnable bit. The real
admission-changing transition requests reconsideration; a Target wake cannot
clear source-credit/Z pressure or bypass output/continuation exclusion.
Target common registration must exist, or its actual concurrent publication
must remain runnable, before any named poll can establish a shared-capacity
wait. This remains the existing Target-readiness protocol, not a new default.

## 37. Finite lane polling and honest Pending evidence

Select a lane-only inbox operation at the existing SessionOwnerInbox Module.
Its caller already owns the checked pair and the selected ordinary transaction;
it cannot receive the other lane's prefetch or fall back to the other receiver.
Preserve the existing mixed recv_next for public/Client/legacy callers with
its exact prefetch priority, eight-Control burst, fallback and two-lane EOF
behavior. Bound never switches between mixed and selected receive on the same
inbox. Characterize any shared-helper extraction separately before behavior.

| Selected unit | Maximum raw receiver poll_recv invocations | Maximum actual receipt |
|---|---|---|
| ControlInbox or OrderedInbox with its retained prefetch | 0 | 1, the original prefetch |
| Selected inbox without its prefetch | 1, only that lane | 1 |
| Driver Control | 2: RESET once, then ordinary Control only if no RESET obtained | 1 |
| Driver Ordered | 1: the DATA/CLOSE FIFO | 1 |

These are primitive invocation counts, not constant CPU/allocator/lock latency.
Pinned Tokio1.52.0 chan.rs289 performs up to two list-pop attempts around one
receiver-Waker registration per raw poll. Control therefore has at most four
such pop attempts. Linked-block advance/reclamation and all containing work
still belong in the later physical Layout/work proof; this table does not
hide them or establish throughput. There is no try_recv/Busy-path substitution,
drain-until-empty loop, task, dynamic future or extra queue.

For a new inbox receipt preserve: store in the selected prefetch, update the
existing streak, commit its capacity epoch, notify, then return that original
receipt. A notification unwind leaves it in that same prefetch; re-entry does
not dequeue or increment the epoch again. For Driver success the existing enum
conversion is a plain move; install Held before validation, notification, another
poll or yield. Keep the selected root/ordinary unentered/entered distinctions
from the turn contract. No receipt is exported for a caller to reduce later.

Tokio returns a consumed channel permit before returning the value. The
existing private try-only producer proof must continue to exclude async sender
waiters whose wake could panic before the owning prefetch/Held commit. Retained
prefetch alone cannot repair that earlier callback gap. Full/Closed remain the
send transaction's exact owning outcomes, not receive-poll results.

Selected inbox Ready(None) proves only that physical lane closed and drained;
it must not close the other lane. Driver Control needs both physical receivers
to report Ready(None). Whole mailbox close still orders terminal state, both
raw receiver closes, then capacity Notify; later probes retain queued/prefetched
commands. Existing bulk-draining TCP terminal delivery is not the bound
retirement operation and cannot replace these charged probes.

### Remove early cooperative deferral only at each raw receive primitive

Explicitly extend the already-selected Target primitive restriction to these
Owner receive primitives; it is not inherited automatically. Each raw
receiver.poll_recv is polled once inside its own stack-pinned concrete
unconstrained(poll_fn(...)) borrow, then that wrapper is dropped. Driver Control
may perform two such independent calls. Never wrap the complete lane operation,
prefetch/Notify, Source transaction, loop or root poll. Preserve the root's
own finite budgets/yield; no stored/dynamic future, per-poll Box or worker.
This wrapper choice remains conditional on compiled scope/build proof before
implementation acceptance, and does not alter public mixed receive.

unconstrained bypasses cooperative pre-registration deferral only. Tokio
trace_leaf can still defer during active taskdump capture; the exact resolved
build must exclude its active predicate (tokio_unstable, taskdump, rt, Linux
and the supported architecture), or this receive proof fails closed. Cargo's
current full/test-util request alone is not a cross-target resolved-feature
proof. No build flags are changed here and no Cargo/compiler gate was run.

Even after excluding those early paths, Pending is not a promise that the
Waker physically remains cached. AtomicWaker registration can race a wake that
takes the cache or immediately publishes. Require registration OR that real
publication retained by the same source generation/revision and root dirty/
clear-recheck protocol. Pending alone cannot clear a due request, prove queue
empty, reclaim publisher backing or justify park. A serial closed producer
graph helps prove exclusivity but does not replace the publication ordering.
Actual Owner-controlled Waker clone/drop must remain bounded, non-owning and
free of business callbacks; no arbitrary caller Waker enters these raw polls.

## 38. Real guarded operation and activation: the remaining coupled work

Same-Reviewer read-only feasibility review found no conflict with a finite P3
private-fixture route, but did not approve an executable freeze. The permitted
relation is: fixture assembles real owning objects; one production-shared
owning operation crosses the actual actor first-failure and output transaction;
checked Controller drives its original Executor/Driver; shared code retains
the result and performs all exit/failure/notification handling. Existing actor
closure may be internal Implementation, not a fixture-injected business callback.

To claim scheduler-driven actual Owner work, production-shared code must also
own generation/source selection, budgets/cursors, pre-call claim/Running,
execution, exact pressure/unknown disposition, post-guard notification and
claim finish/dirty-recheck/park. Tests may construct objects and provide input;
they cannot stitch those business transitions together or replace missing
service with uncharged cleanup. A separately proved shared source operation
is a prerequisite, not completion of that entire scheduling chain.

The current missing ports are concrete, not defaultable:

| Required work | Existing real owning surface / missing link |
|---|---|
| Output/continuation | Output root has the page/permit; actor's existing physical-parts operation supplies its exact FIFO. Existing complete caller is test-only B composition, not CurrentOnly service. |
| Recovery | Controller owns its transferred phase; existing B service requires actual EstablishedLeg/FIFO/active-B destination. Its installer is explicitly legacy policy and cannot be ungated as production A-to-B settlement. |
| Lifecycle install/expiry | OwnerPhysicalLifecycleSlots retains exact opaque actions, but has no actual clock/deadline registration or normal due consumer. settle_for_stop is terminal discard, not normal timer expiry. |
| Target/common/Flow work | Actual Target and retained records are under Executor; demanded/common polls and indexed retry/retirement are selected protocols, not existing callable implementations. |
| Process activation/lookup | Command Root does not currently own Controller/output/actor/FIFO. CurrentOnly has some physical resources but not the full service/lifecycle fields or caller. |

P3 can freeze and test a complete shared finite operation with real owning
fixture resources. P4/P5/P6 still own actual bootstrap/table, whole-root fused
adoption and lookup/caller integration. Do not solve lookup with a second
logical Controller table, borrowed arbitrary Driver/provider, optional-port
dispatcher, generic business callback, raw getter, fake parent or ungated B
policy. Exact shared-operation Interface and actual guard/consumer pairing
remain to be frozen, not supplied by this feasibility result.

Sections36–37 describe local work/registration obligations, not global class
activation. Metadata Bound or an initial due bit cannot publish Session before
its actual owner/ports exist. The eventual actual publication transaction must
prepare all fallible activation requirements before final fallible output-root
construction; no later fallible activation/setter can fill the gap. This must
be reconciled with the real parent and retained cold/associated failures, not
asserted via an illustrative token. No P4/P5 grant or fictitious parent here.

Future falsifiers: external same-pair producer/delivery guard; inner enqueue
followed by outer recoverable Err without work; two coalesced sends cleared
after one receipt; a fresh mailbox given fake OPEN/readiness; cold live source
left unprobed; client-only OPEN counted as Owner source; closure without send
losing drain work; selected receive consuming the other prefetch; cooperative/
trace Pending called registration; a concurrent wake erased by finish; Notify
before receipt custody or while an outer guard is held; a source-unit wrapper
covering multiple raw polls/Notify; tests implementing scheduler finalization;
output after-new fallibility or lifecycle discard claimed timer consumption.
No Rust, tests/compiler/Cargo/fmt/governance, external operation or commit/push.

Same Heisenberg sections36–38 review: P0/P1/P2=0/0/0, Approve only this
conditional initial-work/lane-poll protocol and explicit feasibility boundary;
Module/Clean/System/DDIA9/9/8/8, necessary optimization none. Reviewed2081-line
SHA `90d31217ac65696592986afbbc21a0d5b6096fc0e957fc494f4f7d8a3bb3885f`.
Original1863-line prefix, six companions,19 local links and338 Rust/governance/
HEAD/index independently verified. Immutable entry/audit-draft artifacts are
under `/tmp/mini-vpn-p3-initial-work-AxdFlF/`. Actual build conditions, shared
owning-operation Interface, complete exit graph, guarded ports and global
activation remain open. No joint freeze, Rust/P4–P6 grant or full-P3 claim.
Same Heisenberg final document-consistency verdict:0/0/0 Approve. Seven
fingerprints/48 links,338 Rust/governance/HEAD/index and inverse2081-line
reviewed draft verified; original1863 joint/2880 result prefixes preserved.
Only this factual joint/result transcription and immutable audit-final sealing
are permitted without recursive review. No semantic or implementation grant.

## 39. Real-port audit: two rejected assumptions, phase amendment required

2026-09-08 read-only feasibility result, not an implementation freeze. Same
Heisenberg found P0/P1/P2=0/2/0 against the proposed universal-actor/shortcut
hypotheses. Prior scoped acceptance remains intact; these two P1s are open.
Limited feasibility scores are Module/Clean/System/DDIA8/9/7/8, not full-P3
completion. No Rust repair, compiler failure or runtime regression occurred.

First, Running actor cannot be the precondition for every local operation.
The real actor port at quinn_leg_actor.rs2511 requires Running, incarnation,
IO and FIFO. But owner_initial.rs1010's actual LeglessRootParts owns a joined
actor, original Controller, high-water and terminal-stamp pairing, not a
Running actor. Its consuming transfer at1582 requires applied/correlated
terminal, no active handoff, joined actor and retained stamp/high-water.
With no successor leg, real resume expiry/Target settlement must still have
an owning path. Requiring a new Running actor makes that path permanently
ineligible. Legless is nevertheless not permission for every live operation.

Second, weakening controller_failed is not that path. continuation.rs223
blocks every terminal other than Awaiting; hard_terminal.rs58/116 Applied
retains the discarded result and exact stamp with high-water=1. In addition,
owner.rs912–929 permanently commits Stopped/Fatal on the audited empty output
before terminal application. Rebuilding via continuation.rs192 new would
initialize fresh failure/recovery/terminal state, not transfer those owners.
LeglessRootParts has no Controller extractor. An ungated test adapter, renewed
Awaiting, recreated output root or copied zero inventory cannot close this.

Controller-local and actor-bound complete owning operations may belong to the
same deep Controller/output Module. That is a compatible direction, not
selected signatures or permission to add optional physical ports. Section38's
actor-crossing description applies where an actual physical critical section
is required; it is not proof that every category can require Running. The old
B Continue guard at physical_root.rs373 remains unchanged even though its
closure does not use IO/FIFO. Removing that guard needs its own causal proof.
Local work still obeys the same ordinary/continuation/source-only exclusions,
entered-state custody and deferred notification; no caller-side finalizer.

Before a callable Interface can be frozen, amend the exact owning phase graph
in this joint packet: healthy Active/Legless, Applied with original stamp and
high-water, stopped output, actual failure and unknown/quarantine. Identify
each original owner before/after every permitted transition, exact rejection,
pressure, recovery-wins, expiry-wins, interruption and shutdown disposition.
No silent reopening of existing Fatal/Quarantined state or loss of its history.
Actual table/adoption/lookup remains P4–P6; a private P3 fixture assembles real
objects but cannot supply missing production migration or settlement logic.

## 40. Timer provenance constrains that same phase amendment

Mencius's source audit distinguishes three actual producers, not one generic
timer-ready bit:

| Producer | Actual retained fact and missing correlation |
|---|---|
| Ordinary LegLost, resumable/session.rs1682 | The reducer validates CommittedLeg and outputs ResumeGraceStarted with only generation. The output has no original leg capability, start time or deadline. |
| Initial/recovery acceptance loss, supervisor.rs2240 / owner_target.rs3862 | The terminal transaction consumes its ResumeGraceStarted effects and retains the original event in OwnerLeglessHighWater with exact Executor identity. It does not route that effect through the normal lifecycle slot. No start/deadline is retained. |
| Flow terminal, resumable/session.rs709/2997 / owner_target.rs4673 | Exact TerminalGrace carries full LocalFlow and tombstone identity; the eventual output action retains it after Target terminal work. Output installation time is not automatically original transition time. |

Controller currently holds the recovery high-water and drops it on successful
replacement acceptance (owner_session_controller.rs1868). Its normal expiry
consumer is absent. Executor's expire_legless_high_water at2503 validates
identity, Legless, generation and admission, returning the original capability
on precommit rejection; its success uses effects-only apply_event, with no
deadline test. It must not bypass sections2–7's original observed expiry fact.

The real output FIFO→one lifecycle port→installed slot→physical slots chain
already has exact restoration on capacity/rejection/unwind. Preserve it.
OwnerPhysicalLifecycleSlots stores three scalar actions and at most Z terminal
actions, not deadlines, timer registrations, due selection or normal generation
advance. Its stop settlement discards actions; it is not normal expiry.
SessionMonotonicClock supplies same-Arc identity and fallible now(), and
SimTime has checked_add. Neither specifies Owner grace duration/start policy
or a retained due registration. Existing QuinnTerminalGraceTimer's deadline/
inbound arbitration consumes ClientSessionController; it is not an Owner port.

The amendment must correlate the actual capability, one genuine start and the
same clock without reconstructing any from scalar action fields. Define real
deadline registration, output-excluded due retention and exact stale outcomes
when recovery wins or session expiry invalidates all remaining Z tombstones.
After reducer expiry, keep the committed observation/error rather than restore
an unconsumed timer. Terminal expiry is valid without Active but still requires
the exact tombstone; UnknownTerminalGrace alone is not authorized stale cleanup.

Charge installation's current Z duplicate scan, retained F completion selection,
resume clearing, actual expiry/Target operations, timer/publisher retirement
and simultaneous old/new backing. Existing Target-open C_timer=2*C_open is not
an Owner grace budget. Do not assert Z<=F or hide added work behind K=13.
Future decisive tracers need true before/at-deadline observations, recovery
before expiry, output pressure, stale generation/Z and unknown custody, through
the shared complete operation; not a supplied due=true or fixture cleanup.

Entry `/tmp/mini-vpn-p3-owning-port-Qx4WEj/entry.json` revalidated the prior
sealed packet and1156 repository files/338 Rust/HEAD/index. Main owns this
record; both audits were read-only. Next work is the coupled phase/expiry
amendment, not an implementation grant or another claimed port closure.
Same Heisenberg factual-record review:0/0/0 Approve only this record. The two
design P1s remain open at0/2/0. Seven fingerprints/48 links,338 Rust/governance/
HEAD/index and original2096 joint/2928 result prefixes independently verified.
Only this factual joint/result transcription and immutable audit-final sealing
are allowed without recursive review; no phase repair or expanded authority.

## 41. Select the existing post-join whole-custody entry, not Fatal reopening

2026-09-08 entry/retention selection, independently accepted0/0/0 at this
finite scope. It is not a complete phase protocol or Rust implementation.
Sections39–40's two general phase/expiry P1s remain open. The selected first
finite prerequisite accepts only an already JoinedCorrelated initial branch;
it does not make join a precondition of every session's eventual expiry.

The source-owned chain is owner_initial.rs1176's consuming Accepted→tracked,
the612/752 terminal transaction,1446's actual joined/handoff stamp comparison,
then1582's consuming transfer to QuinnOwnerInitialLeglessRootParts. The final
Parts at1010 owns the actual joined outcome and surviving stamp, original
Controller/Executor/high-water, private pairing witness, generation and reason.
No Controller/output root has existed on this branch. Initial Applied is not
recovery-B Applied: the latter has already committed old output Stopped/Fatal.

Select one first output adoption of that whole Parts into the existing deep
Controller/output Authority. Its new finite JoinedLegless state retains the
actual joined outcome and pairing provenance; the original Controller still
owns its high-water. No new long-lived parent, running actor owner, session
table, CurrentOnly field or P4/P5/P6 caller is selected. This is not a wrapper
around a separately extracted Controller, and not a second failure ledger.

The current sibling privacy does not permit this move today. Freeze a closed
same-composition consuming transfer plus exact whole-input failure return;
the production operation, not the fixture, must own unpacking, preparation,
rollback and final publication. Only the minimal declaration ownership needed
by that complete transaction may change. No raw Controller getter, externally
assembled tuple, extra re-export, pass-through finalizer, optional physical
port or provider is allowed. Exact Rust shape and deletion proof remain open;
this selection does not assume current private fields are accessible.

Cold adoption must preserve these distinctions:

- Validate the actual Parts producer/correlation, joined outcome/stamp,
  Controller health and same current Legless/high-water identity. A copied
  generation/count or a new pairing token cannot replace original provenance.
  Joined transport terminal evidence remains owned even when Controller is
  healthy; do not require a historical peer failure to disappear.
- First output construction means no predecessor output authority is being
  discarded. The closed Bound caller/entry proof must also exclude an escaped
  ordinary page or outstanding continuation before adoption; a zero snapshot
  is not that proof. Preserve the selected cold live/recovery cases elsewhere;
  this new entry does not relabel them or silently narrow new_checked.
- All modeled validation, clock/deadline preparation and required storage
  reservations precede final output allocation. Any rejection/pressure returns
  the same whole Parts and original policy inputs; do not rebuild Controller,
  high-water, joined outcome, stamp or source factory. Temporary empty backing
  may be released. Process allocator abort is not a modeled carrying error.
- On output allocation success, publish JoinedLegless by plain infallible
  moves. No subsequent validation, allocation, setter, await, callback or wake
  fills a gap. A real preparation unwind must retain the original owning
  input in the entered transaction's closed failure custody; a copied audit
  or guard that already lost an object is not rollback evidence.
- Old B Applied/Fatal/Quarantined is not an input of this constructor and has
  no upgrade/reset exit. Existing initial pre-join custody also remains in its
  original owning branch; no new spawn/abort/join machinery is imported here.

## 42. Post-join phase and expiry constraints for that entry

The finite state graph is first-adoption preparation → JoinedLegless, with
exact original Parts on modeled precommit rejection. Subsequent Controller
session expiry must retain sections2–7's original fact, not replace this root
or erase its joined/stamp history. Unknown/failed work remains an owning
non-serviceable state. These are entry/retention selections; the callable due
consumer and full Active→Legless phase graph are still not frozen.

| State / event | Required owning disposition |
|---|---|
| Healthy JoinedLegless | Controller-local work does not require Running. It still obeys ordinary/continuation/source-only exclusions. Actor/FIFO work cannot substitute a joined outcome for a live physical port. |
| Deadline not reached | Keep original high-water, joined/stamp and absolute deadline; park needs actual timer registration or a real retained publication. Neither joined=1 nor an installed action means due. |
| Due while output/recovery excludes reduction | Retain due work and the original capability without restarting grace or minting another timer. Exact eligible output/local settlement must precede consumption; no fixture-side cleanup. |
| Actual observed expiry commits | Move the original expiry fact into Controller's SessionTerminal before later cleanup. Keep joined/stamp custody and committed result/error; do not return an unconsumed high-water after commit. |
| Recovery has actually committed first | Supersession must be part of that actual owning transition, including the original high-water and deadline/registration disposition before later queue pressure. Generation equality or a stale-error code cannot authorize a drop. |
| Output, Target or preparation becomes uncertain | Keep exact original result/error/remaining work in the existing failure authority; never fresh Awaiting, Active or clean zero. |
| Shutdown / destruction | This entry proves no destruction-surviving parent or whole-root zero. The joined outcome's zero actor inventory does not settle Controller, source, Target, output, timers or session authority. |

The actual joined stamp still exposes canonical first-failure observed-at
(quinn_leg_actor.rs892/966); its original actor shares Controller's clock by
initial adoption at owner_initial.rs1683. Thus the original time evidence is
not lost, but its existence is not a selected grace policy. The next coupled
timer decision must choose the genuine start (and its semantic rationale),
positive bounded configuration, checked absolute deadline and same-clock due
registration. It must never restart grace at join, adoption or output install.
An already-past deadline after adoption remains past, not a fresh interval.

Before actual expiry is callable, close both no-successor service and the
real replacement/expiry arbitration. A Controller snapshot with no recovery
does not rule out an actor-held admitted/pending replacement. The physical
caller/cutoff and immutable-arrival rule cannot be supplied by a due boolean
or a test-only no-B assumption. This entry does not select that missing rule.
Terminal-grace start/capability, recovery invalidation, stale Z after session
expiry, output pressure, timer charging and cold peak remain in the same
coupled gate; Owner timers still cannot borrow Target-open capacity.

The initial branch's current join waits for real actor tasks without a bound
relative to grace (quinn_leg_actor.rs3547; owner_initial.rs1529). Pre-join
Pending/failure therefore remains a separate unresolved owner responsibility.
Do not route every expiry through this post-join entry. Likewise, consuming
high-water before current initial join would violate1402/1582's high-water=1
checks; that requires a complete earlier phase amendment, not this constructor.

Future decisive tracers: whole-Parts rejection/pressure identity; old Fatal
entry impossible; joined historical peer failure retained; true before/at/past
deadline; rejected precommit expiry returns original capability; committed
expiry error retains original fact; output exclusion retains overdue work;
recovery before expiry and actor-held replacement; pre-join remains nonzero.
All shared business transitions stay in production code. This prerequisite
does not replace the full-P3 scheduler/activation and P4–P6 separation.

Entry `/tmp/mini-vpn-p3-joined-legless-O22rae/entry.json` validates the previous
seal and1156 files/338 Rust/HEAD/index. Main selected this one source-backed
entry after bounded body/caller audit; Mencius and the same Heisenberg were
read-only. Written amendment and compact-document review passed0/0/0. No Rust,
compiler/tests/Cargo/fmt/governance, external operation or commit/push.

Same Heisenberg reviewed the2312-line draft at SHA
`606d411aa709071c0bfb1f6138c39f2fa179ac12126abb1f707c71102544aa22`:
P0/P1/P2=0/0/0, Approve this entry/retention selection and seven-doc consistency;
Module/Clean/System/DDIA9/9/8/8, necessary optimization none. Original2196 joint/
2968 result prefixes, seven fingerprints/48 links and338 Rust/governance/HEAD/
index independently verified. Private Move shape, actual unwind holder and
callable expiry remain unproved; both broad P1s remain0/2/0. Only faithful
verdict/status/evidence transcription and immutable audit-final sealing may
follow without recursive review. No protocol expansion, Rust or full-P3 grant.

## 43. Owner semantic-loss start and an original absolute eligibility window

2026-09-08 finite policy selection; written amendment review accepted. The same
Reviewer's closure verdict is0/0/0 for start/eligibility/winner selection,
not bounded arbitration or callable expiry. Sections39–40's two P1s remain.

Choose S at the genuine Owner semantic LegLost transaction, not physical join,
output construction or observation. After actual correlation/phase/capacity
preflight, sample the established SessionMonotonicClock immediately before
the semantic loss commit and prepare checked D=S+G. This is a real precommit
sample, not a claim that clock read and model store are one instruction.
The source at resumable/session.rs1682 distinguishes a genuine transition
from an already-Legless no-op; the latter must preserve the original window,
not resample or rearm. A failed precommit attempt creates no new window.

G is explicit positive finite checked configuration, with checked nanosecond
arithmetic and same-clock deadline conversion before semantic commit. There
is no default500ms: the earlier blackout/replay coverage target does not set
Owner RESUME grace. The owning window retains its original clock, configured
G, S, D and expiry capability/correlation through retries and transfers.
Changing configuration cannot restart an existing window. Once past, D stays
past at join/adoption. Missing time on a Legless input is not healthy NoTimer.

The joined actor stamp remains physical first-failure/correlation evidence;
it is not S. Actor-first-failure is not a uniform start for supported inputs:
LegTransportReporter::report at leg.rs132 has no clock, and the real direct
reporter→lose_before_acceptance→Legless bootstrap test at
owner_target/tests/r7_initial_bootstrap_handoff.rs139 has no actor stamp.
Supervisor's producer at2239 and Executor's discard at owner_target.rs3853
currently carry an expiry capability without semantic time. Amend the actual
loss producers and owning carriers together before using this policy. A
later constructor may not retrofit S from now or from a physical stamp.

All selected ordinary, initial-A, recovery and pre-Controller loss paths must
retain the same original window with the actual high-water. A non-actor path
needs genuine owning clock/configuration context, not a caller-supplied
SimTime, fake actor or actor-only input restriction. Preserve supported cold
live/recovery inputs and their original ownership; existing untimed tests
remain characterization, not proof of the new Bound due operation. Retirement
of an old A after B installed does not start a new grace for B.

Clock/deadline failure must return the actual current-phase input. Physical
terminal/FIFO cleanup can already precede semantic LegLost: do not promise
restoration of a Running actor, pre-cleaned FIFO or consumed AttachedLeg.
Before terminal binding, a borrow-only correlation check may precede timing
preparation; after binding, retain the actual pending loss/terminal custody.
Unknown or postcommit failure keeps the entered owning phase and original
fact/window, not a rebuilt high-water. Exact producer signatures, storage,
unwind holder and all cold-caller exits still require joint proof before Rust.

## 44. Arrival eligibility, actual recovery commit and the pressure hard stop

D is the arrival-eligibility cutoff for that Legless window. It is NOT yet a
settlement-completion or resource-retention upper bound. Physical failure→S
latency is separate as well. Selecting fixed D does not prove bounded complete
RESUME grace, bounded shutdown, or a G-bounded end-to-end recovery interval.

An actual complete registered-B record admitted on the same clock at A<=D
gets classification/settlement priority before expiry, including equality.
Arrival alone is not recovery success: registration, authentication, exact
model generation and real capacity preconditions remain mandatory. Preserve
the real encoded/decoded stage, bytes/permits, ordinal, original arrival,
clock and generation/window correlation on every retry. LegBoundFrame alone
has no arrival; decoding cannot justify reconstruction of that evidence.

Include actual actor-held queued input and publication-in-flight records.
quinn_leg_actor.rs4561 explicitly protects any in-flight admission at/before D,
even when its epoch exceeds the timer's frozen high-water. Do not substitute
a cutoff-only scan, empty queue or Controller.recovery=0. The expiry commit
must linearize with real admission/publication and the candidate/Controller
state. A>D is not a winner for this window and does not restart D; it remains
owned until its actual disposition. A committed expiry fence cannot be
revived by a later admission whose coarse clock value happens to equal D.
No grace-relative policy is invented here for an Active/no-window session.

| Actual disposition | Required consequence for this window |
|---|---|
| Installed CAS/model transition | Recovery wins here, not at later acceptance enqueue. Supersede original high-water and its timer in that same owning transition; retain actual pending acceptance. Later errors cannot restore the old window. |
| Precommit pressure or unsettled registration | Retain original request/stage and window. Broad Rejected is not proof of permanently invalid input, and no progress bound follows from retryability. |
| Resynchronize | The request has been consumed; retain the actual generated status publication and its output-settlement obligation. It is not an installed replacement, nor an original request available for return or retry. |
| Authentication/model rejection | Use the actual consumed/returned input and error disposition. Existing B-adapter Controller errors enter failure; they do not supply a normal nonwinner→expiry route. |
| Rollback invariant or unknown interruption | Retain exact failure and remaining custody; never classify it as benign pressure, reconstruct the input, or clear the window to zero. |

The pressure counterexample is concrete: standby.rs4362 reserves acceptance
before full authentication; leg_io.rs2376 requires an empty eligible FIFO and
no undelivered ordered wait. Such a wait can persist on a live transport.
Thus even a pre-D request that would later fail authentication can first be
resource-blocked indefinitely. Already-delivered wait is retired by the
reservation itself; adding another flush does not resolve the general case.
After Installed, reserved push at leg_io.rs2466 has no normal Full capacity
check: its identity/reservation/endpoint errors are not precommit pressure.
Controller1850 removes old high-water before acceptance enqueue already.

Hard stop: before callable expiry or Rust implementing this expiry/resolution
behavior, close an actual
resource-progress bound OR an explicitly reviewed bounded resolution policy,
the actual candidate-set/no-successor Authority, original early-input/error
disposition and output/recovery exclusions. Fixed D plus indefinite retry is
not a bounded solution. If G is claimed as maximum grace/retention duration,
the pressure bound must close in that same claim, not be deferred. No new
retry count, second timeout, cancellation, fallback capacity or expiry bypass
is selected here. The next slice is that owning pressure/resolution decision.
Pre-join responsibility, complete phase migration, normal due registration,
stale Z, timer backing/charging and shared ports remain in the coupled gate.

Required future falsifiers include genuine loss versus duplicate no-op;
clock/deadline failure before and after physical cleanup; untimed cold input
rejection with original custody; before/at/after-D publication including later
epochs; blocked pre-D input; Installed followed by enqueue failure; real status
and rejection custody; old-window supersession; and no-successor membership.
These are design predictions, not executed tests or a full-P3 completion.

Same Heisenberg's written review first returned0/0/1 for the Resynchronize
request/response ownership wording. The one-sentence repair passed0/0/0
Approve, Module/Clean/System/DDIA9/9/7/8, with no other required optimization.
Reviewed joint2433 SHA
`a1d87a722162efefa0c76430c64b5262fd635765964b8d6422e6117400663bde`;
single-line inverse, seven fingerprints/48 links, historical prefixes and338
Rust/governance/HEAD/index independently verified. Only faithful finite
verdict/status/evidence transcription and immutable audit-final may follow
without recursive review. Both broad P1s and pressure/candidate-set gates
remain open; no callable expiry, full joint, Rust or full-P3 acceptance.

## 45. Select owning admission before the deadline fence

2026-09-08 architecture selection; written review accepted. Select direction A:
deadline-fenced arbitration with an explicit normal nonwinner disposition.
Do not select indefinite Held retry as its bound. This does not yet freeze the
attempt count, decision/expiry work bound or a callable operation. A's earlier
freeze review was0/2/0: the normal disposition and its complete owning carrier
were missing. The two broader phase/expiry P1s also remain open.

The new producer graph starts at the existing consuming initial association,
not QuinnOwnerStandbySessionComposition::adopt_registered. The actual capability
is ExactAuthenticatedStandby, not a new candidate type. Supervisor1334's
read-only authentication can mint multiple externally held candidates; a
capacity-one OwnerStandbySlot limits that instance, not the whole Controller.
The candidate's seal/endpoint/authentication and private hint registration
identity are not a Controller allocation-origin or membership lease. The old
B-only adapter's same-clock check is also not that association proof.

Choose this original-holder migration inside the existing owner composition:

- Begin with the sole initial A/Controller aggregate and the actual B actor,
  including its existing queued/held registration and arrival/publication
  evidence. Acquire whole B custody before authentication or any eligible
  registration publication, rather than leave the actor with a naked caller.
- The existing initial composition remains the A/current-port owner. Consume
  the seed once: its original A actor/handoff move to that outer holding phase;
  its one original Controller moves into QuinnOwnerControllerOutputRoot.
  The same subroot owns the complete B admission association, including actual
  candidate, registration retry, slot/receipt/FIFO and later Installed carrier.
  It must not retain the intact seed beside a second copy of its Controller.
- Replace the initial holder's direct Controller field with a mutually
  exclusive Controller-only versus output-associated phase. The latter owns
  the actual current port plus the output subroot, not a raw Controller getter
  or a separately published tuple. Keep the original acceptance settlement and
  shutdown evidence in their actual phase; do not reset them at conversion.
- A terminal/pre-join/joined migration must move all of the original physical
  port, handoff or bound/discarded successor, stamp, joined outcome and exact
  Controller correlation together. A prebuilt terminal root that itself owns
  C cannot be retained unchanged beside the output subroot that now owns C.
  The new normal Legless branch cannot reuse/reopen old B Stopped/Fatal state.
- The initial holder remains a finite initial holding/transfer composition,
  not a new destruction-surviving physical parent. Any later transfer must
  consume its whole current phase, including A retirement and the actual C/B
  subroot. No orphan A, detached join, new session table or CurrentOnly field/
  wiring change is authorized by this selection. A required later caller or
  lifetime expansion is a separate explicit architecture stop, not a wrapper.

The Bound producer graph must keep authentication, registration enqueue,
carrying admission failure and retry within this same original association.
Legacy/test read-only authentication or naked candidate/retry exits cannot
remain reachable from it while the original C resumes ordinary service.
NoB must be constructed from that closed graph, excluding preparation,
external pending capabilities, registered input, gate/rollback and retirement;
slot=None can mean an entered ATTACH gate, not vacancy. This is a required
owning admission migration, not proof that the current producers are sealed.

Do not force existing Legless/non-actor/recovery cold inputs into an initial-A
seed. Preserve their genuine whole-input path and errors; the new initial
entry does not replace or narrow them. Their candidate/absence correlation
must be established from their actual producer graph before due service.
Private placement, complete field/phase migration, closed caller/deletion
proof and exact internal visibility remain to be frozen before Rust. Do not
assume sibling private types are nameable or solve that with an extra export.

## 46. Construction and admission have different return boundaries

Prefer completing first output allocation before B registration enqueue in
the new initial path: rejection of that allocation can return the original
whole authenticated association, actual B and retained input/configuration.
All real correlation, same-clock, capacity and input-phase validation precedes
that final allocation; success publishes the output-associated holder by
infallible moves. Authentication may already have consumed its request, so
this is not a promise to reconstruct a prior encoded registration record.

Once registration or another irreversible operation has changed the FIFO,
the return is the actual entered phase. The complete association stays owned:

| Boundary | Actual retained/returned objects |
|---|---|
| Before admission mutation | Original whole initial aggregate, actual B, its current raw/decoded input and original configuration. |
| Registration enqueue succeeded | Original A/C association plus actual candidate-in-slot, acceptance receipt and FIFO; not an allegedly unadmitted candidate. |
| Admission pressure or rollback rejection | Actual candidate or whole retained rollback, still paired with A/C/B. Store owning rollback before fallible validation. |
| Registered ATTACH Installed | Original C's actual recovery/acceptance and actual B; old high-water/window cannot return after commit. |
| No-ACK input discard committed | Actual returned decoded value plus original correlation/history and retiring actor/slot/FIFO; never an allegedly unconsumed Held/record. |
| Unknown interruption | Original entered owner and every remaining object, with non-serviceable unknown state; no rebuilt input, terminal or clean audit. |

Existing already-admitted inputs must use their real phase; they cannot be
rewound merely to fit the preferred construction order. Whole-transfer
rejection and unwind proof are mandatory, not inferred from copy-only counts.

## 47. A normal no-ACK branch, not a relaxed hard-terminal guard

Select a separate normal disposition branch within the existing associated
Owner retirement authority. Its synchronous entry excludes this candidate's
Installed right for the old window while retaining all original ownership.
This fence is not yet SessionExpired, a transport failure or physical zero.
Installed before entry permanently wins. Wrong correlation/precommit rejection
returns the actual unchanged input; no membership claim comes from a borrow.

The required phase sequence is:

1. Enter the owning normal-decision phase before irreversible input consumption.
   Preserve original arrival, ordinal, epoch, clock/window association and
   actual encoded/decoded stage. Include queued/held/in-flight participation;
   a frozen epoch alone does not close the candidate set.
2. Consume the exact input without sending its reserved DELIVERY ACK, retaining
   the actual decoded return value and correlation/history. The existing
   Held::try_shutdown_discard mechanics at actor1503 demonstrate the required
   consumption, but its shutdown-only policy is not ordinary pressure authority.
   Define the normal decision explicitly; do not merely rename its use. Returned
   decoded value/bytes remain in the owning inventory until actually consumed.
3. Request genuine cancellation while the same owner retains the actor. Wait
   for actual terminal publication using capture/check/register owner progress,
   not a retry loop or a newly spawned join task. Drop notifications must not
   escape while Root/actor/Ready guards remain held.
4. Bind the real reported terminal to runtime→LegIo→sole FIFO and exact slot,
   then retain the actual retired slot, queue inventory and original stamp.
   A modeled rejection restores the whole terminal envelope. Unknown unwind
   after terminal extraction is not that modeled rollback.
5. Only after FIFO retirement, directly observe join on the still-owned actor;
   retain the exact result and correlate its stamp with the actual handoff.
   Cancellation of observation cannot drop the actor or result. Do not import
   the old initial/standby spawn-and-abort join machinery into this branch.

Reporter publication follows child/inbound drain and counters.wait_zero at
actor5960, but precedes connection/endpoint drain and the final join outcome.
Keeping Held/permits until reporter would self-deadlock. Conversely cancel
returning does not mean terminal is already available. The existing reported
terminal/FIFO/slot primitives accept a real LocalCancelled, whose physical
failure timestamp is None; the separate old hard-terminal branch rejects it
and the test adapter stops output Cancelled for UnexpectedLocalCancellation;
other rejections call fail_output. Keep that old branch unchanged. If a real failure wins
first cause, retain that reason/stamp rather than manufacture LocalCancelled.
Neither no-ACK discard nor terminal cleanup marks an ordered wait Delivered.

This sequence defines ownership ordering, not a grace-relative physical time
bound. Before callable expiry, the same coupled gate must still freeze the
complete candidate admission/fence, one-way Installed/nonwinner/error matrix,
finite charged decision and semantic-expiry work, output/probe-timer migration,
and actual shared due consumer. No infinite wait hidden in Retiring may count
as bounded resolution. Root-wide physical shutdown still requires its actual
outcome proof; neither a configured deadline nor actor-zero replaces it.

Decisive future falsifiers: an externally retained second authenticated
candidate; wrong same-clock Controller; registration pressure after enqueue;
Held preventing reporter; real LocalCancelled versus prior failure;
notification/terminal-extraction unwind; queued/in-flight arrival at D;
Installed before the normal fence; nonempty output/external probe timer;
cancelled join observation; and valid non-initial cold custody. These are
required future checks, not tests run in this architecture-only selection.

Same Heisenberg's written selection review found one P2 in the old adapter's
Cancelled versus fail_output mapping. The precise correction passed0/0/0
Approve, Module/Clean/System/DDIA9/9/7/8. Reviewed joint2595 SHA
`0735984c13aed9c7c1736e5eb594ef778994ab7dcbf7ed3823f7b5e6162006e1`;
exact inverse, result-only append, seven fingerprints/48 links,2444/3055
historical prefixes and338 Rust/governance/HEAD/index independently verified.
Only faithful finite holder/normal-ordering verdict/status/evidence and an
immutable audit-final may follow without recursive review. Complete phase,
candidate fence, work bound, callable Interface and Rust remain unapproved;
both broad P1s and full P3 remain open.

## 48. Initial placement: retain declaration anchors, move a complete implementation

Status: exact preparatory Move plan accepted0/0/0; not executed. This selects
placement for sections45–47, not their new owning phases or runtime behavior.
It does not change the current seven-document-only write boundary.

The existing composition parent has sibling owner and owner_initial Modules.
Initial cannot name the owner-private continuation Root. Widening continuation,
its Root/constructor/failure and service-return types would violate the existing
privacy/provenance contract. Select inward placement as owner::initial instead.
The C/output Root declaration and all its existing visibility stay in
owner/continuation.rs; placement grants no access to its private fields.

Whole-file relocation is insufficient: owner_initial.rs is2067 lines and its
new path cannot inherit grandfather debt. First extract the complete
accepted-current Implementation into private default-path accepted_current.rs.
Keep every existing type/enum/storage/transaction declaration at initial parent,
including AcceptedRootParts, terminal state/quarantine/root/guard, tracked and
legless Parts. Keep common ActorSlot/Drop, ActorJoined, join helper, seed and
authenticated-pair machinery, plus the complete legacy initial composition.
This is explicitly a declaration-anchor plus complete Implementation extraction,
not a claim that all state declarations move to the child.

Freeze these15 complete impl blocks from the current source, inclusive ranges:

| Implementation | Original ranges |
|---|---|
| terminal state, Debug, cause, root, transaction, Drop |484–554;556–595;597–610;612–631;633–816;818–827|
| terminal drive and joined observation |845–867;913–963|
| legless failure and Debug; legless Parts |983–991;993–1001;1018–1079|
| accepted Parts and Debug |1081–1224;1226–1234|
| tracked-current complete drive/join/transfer and Debug |1236–1652;1654–1662|

The exact blocks contain1072 lines; remaining original parent text995, before
routes/imports/docs/separator cleanup. The source auditor's grouped1082 includes
ten intervening separator lines; it is the same15-impl closure. Neither count
is a final formatted-file prediction. The immutable read-only block/test hashes
are in `/tmp/mini-vpn-p3-initial-placement-mDjE2h/closure.json`; original source
SHA is `6f9480ce09fc3415e27cad07c45a11ec02039c6d4c463ec5d27d03f9044650a8`.
Each new production file must actually remain at most1500 after formatting.

The21 private inherent methods deliberately narrow from initial-private to
child-private because every caller moves with them: state identity/handoff
stamp/consumption/quarantine/failure/pair queries; cause label; root begin and
two retained queries; transaction entry, rejection restore, two quarantine
operations, bound publication, discard quarantine/application and callback;
joined observe; start join and finish correlation. Do not add21 visibility
annotations, public helpers, re-exports or forwarding methods. Trait Debug/Drop
remains implemented for the same parent-defined types. All45 pub(crate)
methods in this closure retain their original scope, including the cfg(test)
probe. No condition, branch, state/error value, Drop policy, order or call body
changes. Existing spawn-and-abort legacy behavior is preserved, not adopted as
the new normal no-ACK branch or certified safe by this Move.

Visibility and path mapping is explicit:

- Extracted into_physical_root_seed (original1195) becomes
  pub(in crate::owned_upstream::quinn_leg_composition), preserving its old
  composition scope, not the lexical child's pub(super).
- During subsequent namespace relocation, the seed declaration and four
  aggregate/seed into_parts/from_parts entries at original345,360,369,388,402
  receive that same explicit composition scope. Other visibility is unchanged.
- owner declares pub(super) mod initial; its effective scope is composition,
  equivalent to the old sibling route. The inner accepted_current route is
  unconditional, inherited-private and default-path, with no public items added.
- Composition's existing five initial re-exports change only their source to
  owner::initial. No new name, alias layer or additional re-export appears.
  physical_session_root.rs24 changes its sole direct seed import to the new
  path. Internal nominal paths change; the five outward names do not.
- Initial's QuinnLegCompositionGate import explicitly names its original
  composition authority after relocation; do not resolve through an accidental
  owner import. Child may use the initial ancestor's fixed private definitions
  and imports. No moved method body contains a super:: path requiring rebasing.

Deletion test: deleting accepted_current removes all actual accepted branching,
terminal transaction/Drop, join correlation and legless transfer operations;
no alternate parent implementation remains. Deleting owner::initial removes
the original initial composition and its five real outward routes, not merely
an unused wrapper. This is structural reachability, not production activation.

## 49. Preparatory sequence, exact route exception and source checks

The implementation plan's468–470 prohibition on reopening owner.rs needs one
explicit exception for this placement. Accepted plan exception: only the initial
Module declaration, paired with one identified redundant structural blank line
if rustfmt leaves it absent. Owner remains at most2062; no compressed unrelated
code/docs/assertions or changed behavior. Net-zero is allowed by the scheduler
amendment637 and governance, but never substitutes for this scope amendment.
If that exact layout cannot be format-stable, stop and select a genuine separate
Move; do not silently use another file or manufacture a shallow helper.
The written placement verdict below accepts only this exact plan exception;
it is not a Rust execution grant or a general parent reopening.

Separate three characterized, green-to-green boundaries; do not batch them with
phase behavior. The following are candidate future write sets, not current
permission. Paths are relative to src/owned_upstream unless stated otherwise.

1. Test-only characterization Move: in
   quinn_leg_actor/tests/r7_initial_acceptance_settlement.rs, move only the43
   lines1126–1168 containing current_terminal_bind_and_discard_share_one_closed_root_transaction
   to a new private default child
   quinn_leg_actor/tests/r7_initial_acceptance_settlement/source_contract.rs.
   Parent adds only mod source_contract; and loses that body; child needs no
   fixture/helper import. Rebase its existing include_str! by one directory.
   Seven runtime tests retain their FQ names; exactly this structural test gains
   source_contract in its FQ route. Baseline and moved exact test plus the full
   eight-test Module must pass. Parent1168 must shrink, new child at most800.
2. Implementation extraction: only quinn_leg_composition/owner_initial.rs,
   new quinn_leg_composition/owner_initial/accepted_current.rs, that source
   contract child, and owner_session_controller/tests/capacity.rs. Move the15
   impls, add the private route and compact ownership/entry/test docs, apply the
   one effective-scope annotation, and make mechanical imports/separators valid.
   Source contract checks declarations/root fields/ControllerDiscard payload
   in parent, callback→bind→discard/bound publication and Drop in child. Global
   RetryBlocked ban covers both. Capacity test390 scans both physical sources
   for LegacyOwnerSessionController/unchecked construction, and preserves the
   initial OwnerTargetExecutor ban over both. Do not concatenate files to fake
   one physical implementation or remove/weaken existing assertions. Existing
   lexical checks are not promoted as a general parser-level proof.
3. Namespace relocation after extraction acceptance: move the two initial
   files into quinn_leg_composition/owner/initial.rs and
   quinn_leg_composition/owner/initial/accepted_current.rs; remove their old
   paths. Allowed mechanical consumers are quinn_leg_composition.rs (old route
   removal/five re-export paths), owner.rs (the exact route exception),
   owner/physical_session_root.rs (seed import only), source_contract.rs and
   owner_session_controller/tests/capacity.rs (actual include paths), plus the
   five visibility anchors and Gate import in relocated initial. No other
   caller or Rust file is in this candidate set. Composition9447 and owner2062
   stay net-zero/shrinking; the moved files each satisfy the normal1500 limit.

At each stage, the sole Writer reports exact final lines. Only Integrator may
edit tests/code_growth_governance.rs: lower shrunk grandfather counts and remove
an entry once it reaches its normal limit. Extraction must remove the old
initial2067 entry before later relocation; no new grandfather path. Expected
shrink-ratchet handoff is not permission for the Writer to alter inventory.
No scanner/repository-provenance implementation changes are included.

Required future evidence: current focused baseline before each Move, exact
inverse reconstruction of all original bodies allowing only the listed scope/
route/import/doc/separator changes, deleted-symbol/deleted-path checks, and
post-Move focused plus initial eight-test Module. Extraction/relocation also
run checked-capacity regression, CurrentOnly constructor/A/B/error/structural
set, continuation and affected owner regressions, both repository/Knife
provenance suites, fmt/diff/check, standalone and Cargo governance, then the
same independent Reviewer. Runtime FQ changes are forbidden; the one structural
FQ change is intentional. Known unrelated actor ordered-wait debt is not repaired
or reclassified by this stage. New failure first gets a causal tree; autonomous
repair stays inside the accepted single axis/files, otherwise stop for authority.

## 50. Boundary after placement

Even a compiled/accepted preparatory Move would only make the required private
owner operations legally nameable. Initial fields/phases, preservation of the
old terminal/handoff/join histories, one C plus complete B association, actual
phase-carrying returns, and valid non-initial cold inputs still need their own
coupled freeze. No private C-field getter or new adoption/callback/provider is
authorized by placement. Candidate-set/no-successor, finite charged pressure
decision/semantic expiry, genuine S/D producer propagation, due/shared ports,
eligibility masks/work/Layout and executable confinement remain open.

The two broad design P1s and full P3 remain open. This exact preparatory plan
has passed written review. Section51 now consolidates the next execution
decision; placement/expiry is not automatically the first Active-path blocker.
Executing even the preparatory Move requires an explicit subsequent grant;
this packet neither supplies that grant nor silently rewrites the full P3 goal.

Same Heisenberg final written verdict0/0/0 Approve, scoped
Module/Clean/System/DDIA9/9/8/8; no additional necessary optimization.
Reviewed joint2772 SHA
`d8feb5267de486d74af3aedf74789bc256aa2e21508fa80e011aacaac683a496`.
The two finite plan findings are closed: explicit parent-route exception and
complete source-check routing. Independent hashes/counts/caller/scope audit,
seven fingerprints/48 links, original2606/3115 prefixes,338 Rust/governance and
HEAD/index all pass. Only faithful finite verdict/status/evidence transcription
and immutable audit-final may follow without recursive review. Actual format,
compile/inverse/gates, execution authority and all broader behavior remain open.

## 51. Consolidated P3 exit: one real Active vertical, then the remaining ledger

Status: user-authorized workflow/scope consolidation; reviewed0/0/0 Approve.
Subsequent explicit user confirmation authorizes early P3 whole-CurrentOnly
residency and actual shared service. This narrowly supersedes the timing of
whole-root storage/caller visibility in the Case C plan section5: use an opaque
owning representation in the same physical Module, with only necessary
owner-scoped operations/value types and an actual simultaneous resident caller.
Fields, phases and from_initial_seed remain private. Reject/pressure returns
the same already-built whole root and inputs, not a fabricated seed rollback;
one inward command owner, paired inbox and ready identity remain mandatory.
No P4 table/bootstrap, P5 PreparingParts/fused seed-to-Live publication or P6
caller is authorized. The same Reviewer approved the scope choice0/0/0. Its
test-only real pre-opened fixture baseline is now green and reviewed. Two
independently executable implementation Moves have also passed source review
and local gates: checked construction under its existing C Authority, and the
complete Target failure/completion closure under its existing Executor. They
preserve behavior/effective scopes and lower exact parent ratchets to3565/10061.
Neither closes the A1–A4 implementation packet or the bound-service tracer.

The selected old-test representation is a cfg(test)-only opaque nominal
OwnerCommandPrototype anchor and resident arm owning the single inward
SessionCommands. Keep the same claim/operation/finish/park loop; production
residency owns only whole CurrentOnly. Complete prototype/test operations stay
inward, with finite same-point assertions, no private-state getter, generic
injection/provider, duplicate commands or reverse Root-policy dependency.
All constructor/anchor/arm reachability must remain cfg(test). Same Reviewer
approved this representation choice0/0/0, Module/Clean/System/DDIA9/9/8/8;
concrete test-operation signatures/placement remain in the shared write freeze.
The shared command-side visit itself is now compiled and reviewed0/0/0 as a
single-file preparatory extraction: SessionCommands::visit_selected_lane owns
the original validation/transfer/rearm/next-mask sequence and returns only
SessionCommandVisit work/masks. Root retains claim/budget/finish/park. Root857,
same56/56 tests, five10/10 repeats and all local gates pass. This is not the
Prototype implementation, inward Move or A1–A4 binding acceptance.
Only the actual whole-CurrentOnly route may prove Bound service.
This section controls the next-step ordering instead of earlier global phrases
requiring every joint row before any Rust. It does not waive an invariant,
authorize files, delete an acceptance item, or redefine full P3 as this slice.
No new sidecar is needed. The full result ledger remains the completion test.

Source fact: server_root.rs510's CommandResident has no Controller/Driver and
poll_ready_unit at698 only transfers/rearms already-held cells. This is accepted
command scheduling, not a connected Target-to-output path. The earliest real
vertical is an Active, genuinely opened Flow through the existing checked
Controller/Executor/Target/Driver and physical output. A late expiry or new B
admission is not required to make that DATA path run; reachable errors and
shutdown ownership cannot be omitted on that account.

### A. Mandatory before the first complete Active-path implementation

| Item / blocker | Completion evidence / dependency |
|---|---|
| A1 Actual owning binding and caller | Freeze the concrete cold same-domain inputs, all affected checked-constructor callers/returns and original factory identity; one pair/observer, two cells inside the actual Executor, inbox inside C, one existing C/output owner. Root must call that owning operation, not borrow an arbitrary Driver. All fallible preparation precedes one final publication. Depends on accepted construction/commands contracts, not a P4 table. |
| A2 Complete source/receipt/output operation | Cut over both actual read and failure/RESET mixed intake paths on Bound service; no alternate indirect write/HalfClose bypass. Receipt enters the original C/output transaction before observed reduction, keeps actual page/completion on error, and reaches actual actor FIFO or retained lifecycle action. Depends on A1 and actual Active actor/controller/FIFO correlation. A fixture may assemble real objects, not run the reducer/scheduler/finalizer for them. |
| A3 Real readiness and bounded backing for this path | Freeze actual Target interest registration, source credit/release coverage, selected lane/category charges, root first-fault/claim/finish/dirty/recheck/park and checked Layout plus simultaneous cold peak for every newly reachable allocation. Full retains its actual command and registration. Depends on A1/A2; no copied-count capacity, default masks, zero placeholders or hot full-map scans. Unimplemented future categories need not be falsely declared proved. |
| A4 Lifecycle, growth and one executable boundary | Name rejection/pressure/observer-cancel/failure/stop dispositions below; freeze exact files/symbols, required prior pure Moves, constructor migration and one failing tracer. Baseline must exercise the real fixture; compile GREEN must contain the complete owning route, followed by lifecycle tests and review. No namespace Move is mandatory solely because it has an approved plan: require a real A1/A2 dependency. |

2026-09-10 execution: "Private cold Open Move and independent CLOSE" is
accepted bounded test-source only; production remains unchanged. User granted
the private flow_lifecycle/cold_open.rs path. The original230-line block
(three tests plus dedicated resume helper) moved unchanged, with an exact
parent inverse and no visibility/Interface change. Parent798->569, child238,
8/8 before/after; same Heisenberg Move verdict0/0/0.

The separate original EOF -> Driver CLOSE -> paired inbox tracer now verifies
one CLOSE/offset0, its own transit1->0 and unchanged StagedReset(full LocalFlow).
Other real Z expiry cannot retry the queued RESET. It directly runtime GREENed
after a test-only ByteOffset constructor correction; no production RED/GREEN.
Same Heisenberg independently approves the test-source0/0/0, scoped9/10.

Final parent646/child238, lifecycle9x10, Target127, exact1/1, Knife162/repository17,
strict standalone/Cargo governance7 and fmt/diff pass. All74 untracked checks
pass. Earlier Root/C/Bound/library checks remain prior evidence. Only the test
parent and new child differ from377 baseline hashes; all production, tcp.rs and
governance752 unchanged. No inventory edit or full A1-A4/B/P3 closure.
Checkpoint: /tmp/mini-vpn-p3-cold-move.zRhPmx/entry.json.

Current Failed-work custody component is SOURCE accepted after same Heisenberg
0/0/0 re-review: original work/cause and feedback remain in one Flow through
real pressure, reduction, source wake unwind and actual terminal. One-step
PeerReset exposed early independent Write release; its scoped RED->GREEN now
defers Target/reset/release to existing terminal-first FlowFinished. Latest
result "Bound Failed work/cause retention and terminal-order component closure"
and /tmp/mini-vpn-p3-bound-failure.9DmOre/accepted.json own exact evidence.
This is not complete Failed phase or selected lifecycle acceptance.

Actual cold/local Failure charge isolation is now test-source accepted0/0/0
by same Heisenberg: real Unbound Target failure/Z pressure, same-factory Bound
installation, another Flow DATA transit1/bytes4 survives actual local retry at
charge0, then its original paired receipt settles1->0. Only authorized leaf
changed; private fixed-Control extraction was separately green-to-green.
Latest result "Actual cold/local Failure Driver-charge isolation" and
/tmp/mini-vpn-p3-cold-failure.lgyx0R/accepted.json bind380 hashes and final gates.
The subsequent2026-09-11 private sink test-path grant and pure Move below
leave parent679/child128; other new test destinations remain ungranted.

Incoming independent Write|HalfClose is component-accepted: separate real DATA
and CLOSE RED/GREEN, same-source0/0/0 reviews, original single sink slot,
no Target I/O while Failed and release only at real FlowFinished. Latest result
"Incoming independent Write/HalfClose sink custody" and
/tmp/mini-vpn-p3-sink-close.KIZhEU/accepted.json bind381 hashes and final gates.
This does not close every lifecycle/pressure branch.

Owner terminal-revoke progress-wake custody is now test-source accepted0/0/0
by same Heisenberg: real queued FlowFinished, fresh waiter, retained exact
terminal/sink/Target/source and charge1 after unwind, stable duplicate refusal.
Existing production directly GREENed; only sink.rs377->523. Latest result
"Owner terminal-revoke progress-wake custody" and
/tmp/mini-vpn-p3-terminal-wake.uoiT5d/accepted.json own381 hashes and gates.
Retention does not prove recovery or selected retirement.

Selected EOF recording is now SOURCE accepted0/0/0 by the same Heisenberg.
Original TerminalFlowSettlement adds Absent/Revoking/Draining with independent
lane facts; actual EOF commits its own bit before summaries/metadata finish.
SourceDrained is a pure work result, not a resource or retirement capability.
The original lifecycle tracer keeps both actual claims/queued receipts and
uses original late Driver wakes to cover repeated selected EOF. Latest result
"Selected terminal-source EOF recording" and
/tmp/mini-vpn-p3-selected-drain.EXTHDs/accepted.json bind381 hashes and gates.

Automatic terminal drain re-eligibility is now component SOURCE accepted0/0/0:
the original two tickets are pre-acquired before terminal mutation and published
after actual successful revoke/Draining, preserving BoundInactive due. Earlier
real selector RED/GREEN proves Ordered. The next accepted test-source closure
"Independent idle-Control wake causality" adds a fresh no-local-RESET row;
Control-only mutation yields OrderedSend != ControlSend specifically there,
and exact restored production returns GREEN. One FQ/two scenarios, same
Reviewer0/0/0, all current gates. Latest381 hashes/evidence are in
/tmp/mini-vpn-p3-control-wake.wIKCwi/accepted.json; no new production change in
this causal-test slice. Neither test uses a manufactured first Driver wake.

The subsequent "Genuine cold-Retiring terminal custody" component is accepted:
actual model Z2/Target tombstone1 installation reproduces missing proof after
join; original Unbound Flow now retains actual terminal/reason in Cold before
Target work, with no premature revoke. Only pure Cold Unbound custody has old
None admission; Bound never does. Same Reviewer SOURCE0/0/0; exact+10 repeats,
OwnerTarget143 both modes and final gates. Latest381 hashes:
/tmp/mini-vpn-p3-cold-retiring.bZTVtG/accepted.json. Parent9581 net-zero,
completion1465/bound.rs800; no new field, ledger, path or inventory edit.

Remaining: actual selected Retiring service, terminal disposition/completion/
join/retry and F/publisher retirement. Original proof retention is now present;
Cold still means not revoked. Freeze the real next transaction/test within
existing scope/limits; global retry is not selected-unit acceptance and
retained Source/F is not zero. The complete C/root lifecycle model/scope
below remains mandatory and ungranted. Only the two paths below are now granted.

2026-09-11 next executable-scope audit0/2/0 led to this precise request;
the user's subsequent `继续` now authorizes its pure Move stage:
new private owner_target/completion/retirement.rs receives the complete current
970-1464 eight-method terminal/join/receipt closure; new private
owner_target/commands/tests/bound/retirement.rs receives the complete current
701-800 cold-Retiring test. Existing parents gain only their private default
routes during pure Move. Existing repository provenance owner_bound_service.rs
must follow the exact route, eight unique methods and preserved effective
visibility; scanner/inventory stay frozen. First characterize/invert/gate and
review the Moves, then freeze selected behavior. No C/root expansion follows.
Later Bound grace release must stop performing global
join/completion work ahead of selected Retirement; preserve Unbound semantics.
Actual cached registrations/tickets and F remain nonzero until their full
selected disposition. C/root lifecycle scope remains separately ungranted.

The authorized pure Moves are now accepted (not selected service): complete
production495/test100-line inverse checks, completion971/child510,
bound700/child108 and existing provenance618. Five method scopes/seven relative
paths preserve the original effective Interface. Same Heisenberg SOURCE0/0/0,
Main full regressions/provenance/governance green; no inventory edit.
Latest383 hashes: /tmp/mini-vpn-p3-retirement-move.QUvYjm/accepted.json.
Freeze the actual selected transaction next; the preceding block's line ranges
describe the pre-Move source, not current locations.

2026-09-11 selected implementation is now WIP, NOT accepted. Latest existing
result: "Selected Retirement/Completion — focused green, review/regression stop";
checkpoint /tmp/mini-vpn-p3-selected-retirement.wiXjXT/stopped.json binds383
Rust hashes/twelve deltas. Actual selected revoke RED->GREEN, normal join/
Source release/F0, cold Open selected outputs, and two EOF waiters after one
real cell vacancy pass. Unbound1 passes; Bound failure stops12/14 on two old
global retry expectations. Heisenberg SOURCE0/3/1 still requires terminal
Returned/Failed release, lawful inactive request, actual physical mask and
exact Join failure retention. No later final gates or source acceptance.

Next precise test placement request (now explicitly granted by the user): private
owner_target/tests/r7_nested_abort/bound_failure/completion.rs, parent default
route and existing repository provenance route/two-symbol locks. Move original
85-line cold_local_failure_retries_without_borrowing_another_flows_driver_charge
and60-line real_z_pressure_retains_failed_work_and_driver_charge_until_actual_grace_release
unchanged from parent799, then migrate their actual claim/Completion/finish.
No fixture/FailOnceTarget/helper/visibility/assertion change. Existing sink.rs
is not a substitute destination for this Z axis. Plan review
01a08fcd-2116-7360-a49f-5b47002f31b8 identified this exact authority boundary;
the latest user grant resolves it without expanding production behavior scope.
Preserve both known REDs through the Move; do not claim a green-to-green result
until actual selected-call repairs and regression/review close. Full C/root
lifecycle installer/clock/deadline/due remains mandatory and separately ungranted.

The granted two-test Move and selected-call migration are now locally closed:
parent653/private child244/provenance646, both-mode Bound failure14/14,
two exact tests10 repeats each, provenance18/18 and both governance7/7.
Same Heisenberg Move/migration/format SOURCE0/0/0. Production0/3/1 is unchanged.
Latest evidence is "Authorized Completion tests — reviewed local closure, next
placement stop" in the existing result, with384 hashes at
/tmp/mini-vpn-p3-selected-retirement.wiXjXT/completion-test-stop.json.

Next precise scope (granted by the latest user confirmation): private
owner_target/commands/tests/bound/flow_lifecycle/completion.rs, parent private
default route plus the complete received_reset_retries_once_after_real_terminal_pressure_releases
test (current618-687,70 lines;71 with separator), and existing repository
provenance route/one-symbol lock. Parent791->predicted721, child77 before repair.
Pure Move must preserve exact0/1; current full all-feature OwnerTarget144/145
is not accepted. Existing cold_open/other-ancestor Completion are not substitutes.
After Move, permit the new child to assemble tested Flow plus three actual
competing Flows before binding via unchanged lifecycle_executor, retaining
original F4/Target4, shared fixture/config/helpers and all original assertions.
Each real expiry wakes the retained waiter; a pre-imported competitor's peer
RESET reoccupies Z before actual selected Completion retries. Prove three real
rejections/retained command+charge before final actual release and success.
Do not use new post-bind OPEN: its service_position is None and dynamic Bound
admission is not implemented. No fabricated F/ticket/wake or global retry scan.
Reviewer01a08ff7-b4af-7e91-b488-dd34688f89e2 confirms this exact boundary;
the latest user confirmation authorizes this test-only plan, not expanded production scope.

The granted pressure test-source slice is now closed: byte-exact70-line Move
preserves0/1 first, then real pre-bind competitors and selected Completion
produce three actual capacity rejections and final once-only success.
Same Heisenberg Move/behavior-format SOURCE0/0/0; parent721/child268/provenance670.
Final exact10 repeats, OwnerTarget145/145 both modes, repository19/Knife162,
strict standalone/Cargo growth7 and scoped fmt/whitespace pass.
Latest result "Repeated-pressure selected Completion — test-source closure";
385 hashes: /tmp/mini-vpn-p3-selected-retirement.wiXjXT/pressure-completion-closure.json.
Resume existing production0/3/1 repairs; this does not close selected transaction
or expand full C/root lifecycle/clock/deadline/due authority.

2026-09-13 inactive-owning runtime RED and finite scope (user-confirmed grant):
original retiring-import1/1 now compiles and fails0/1 at tests/flow.rs:751
Corrupt after one lawful inactive owning request. Production flow.rs unchanged
1432raw/1475formatted; existing test795. The complete in-memory typed Retained
candidate through original publish_bound formats to1571, not compiled/written.
This does not prove every shorter implementation impossible. Main selects a
complete private publication Implementation Move for readable lifecycle proof,
instead of compressing/removing checks or borrowing another test axis's space.
Same Heisenberg causal review01a09da8-3b15-7051-a1bc-e845c021ccf4 confirms the
existing P1; finite scope review01a09db0-f077-7d33-96dc-7552b27b9369 is0/0/0.
Those reviews are not production acceptance. The user's subsequent explicit
confirmation grants the finite three paths, existing routes/provenance, pure
Move and subsequent owning repair/verification described below.

Exact granted new paths (all below src/owned_upstream):

- owner_ready/flow/publication.rs: complete request_retained_flow_work and
  begin_publication, entire OwnerPublicationTurn impl (finish_quarantined/finish),
  entire ReadyShared publication/notify impl, BoundRequest enum and both
  Wake for FlowWake/SessionWake impls. Preserve original effective visibility.
- owner_ready/flow/publication/tests/mod.rs: private default mod retained only.
- owner_ready/flow/publication/tests/retained.rs: complete metadata-owning
  inactive/active, identity/phase/capacity/category, revision exhaustion and
  quarantine retention tests. Use actual reservation/binding/exclusion and
  ancestor-private metadata; no business Authority or TCP service is fabricated.

Existing owner_ready/flow.rs gains only private unconditional default
mod publication plus exact moved Implementation removal/compact Module docs
in the Move. Storage, metadata algorithms, strict receipt validation and other
callers remain there. Child gains #[cfg(test)] mod tests when tests land.
Moving begin plus both finishes keeps all notify_bound callers inside the
child, so no new pub(super), re-export, getter, shim or callback Interface.
Existing tests/flow.rs keeps its actual RED; no cold-test Move is necessary.
Existing tests/repository_test_module_provenance/owner_bound_service.rs may add
exact private/default/cfg routes and type-qualified method/Wake impl ownership
locks, old-parent absence and uniqueness while preserving all prior checks.
No scanner, inventory, other Rust path or physical/C/root expansion is requested.

Authorized sequence: pure Move preserving exact0/1 plus all other
characterizations, inverse/routing/growth checks and same independent review;
then typed owning request under the original single exclusion, with all
fallibility before destructive Live->Retiring commit, no temporary Free or
copied ReadyCell, inactive due/noNotify and original active publication.
Add one actual tracer at a time; compile, run negative/retention/overflow and
Target/callback regressions, provenance/governance/scoped fmt/check, and same
Reviewer closure. Format separately and verify inverse against canonical
original text; do not call a pure Move GREEN while the known0/1 remains.
Predicted parent~1000-1150, child~350-600 (<1500), index1 (<300), testleaf~300-550
(<800), existing provenance670 plus finite locks (<800); measure before writing.
If any full body cannot fit, do not compress or silently add another path.
This finite scope changes no P3 acceptance obligation or completion claim.
The grant is now locally closed: reviewed pure Move1095/395 first retained
original RED, then owning repair publication515/private index1/test555 and
provenance792 passes both-mode OwnerReady25/Target145, ten OwnerReady repeats,
repository20/Knife162/growth7/scoped fmt/check. Same Heisenberg SOURCE0/0/0
closes only inactive P1. Current388 hashes and exact results:
/tmp/mini-vpn-p3-publication-move.EfieqP/behavior.json and the existing result's
"Inactive owning repair — locally accepted" section. Other findings stay open.

2026-09-13 next component is locally accepted: terminal-held Driver Returned
and Failed normal disposition, same Heisenberg SOURCE0/0/0. Existing completion
1143/retirement841/test596; actual charge RED->GREEN and normalized Failed
backing-reference RED->GREEN, three real Reduced/Returned/late-receipt origins,
Target147 both modes plus final scoped gates. Current388 hashes/evidence:
/tmp/mini-vpn-p3-terminal-disposition.vWWYVq/accepted.json and the existing
result's "Terminal Returned/Failed disposition — locally accepted" section.
No fake destructor panic is evidence: legal work has no user callback; future
callback-bearing resources reopen that audit. No scope or acceptance reduction.

2026-09-14 physical Retirement normal-release component is locally accepted,
same Heisenberg SOURCE01a09e1e-9c66-7be3-ab40-7d52e7b6616d at0/0/0. Existing
continuation1254/bound_source600 only;386 other Rust hashes unchanged. Real
prebind graceful terminal/join backlog, original expiry and actual byte consumer
leave cold Retiring for checked C/CurrentOnly/adopt/start. Root-only selection
RED0vs1->GREEN1 and ten repeats establish one actual release, not whole-root zero
or postbind lifecycle. Shared fixtures/three fixed observations stay unchanged.
Current388 hashes/commands: /tmp/mini-vpn-p3-physical-retirement.GA2g7y/accepted.json.
CurrentOnly8/Target147 both modes; all-feature C44/Root24/continuation16/Ready25,
provenance20/162, growth7/7, scoped fmt/check. No new scope or acceptance waiver.

2026-09-14 Completion feedback-loop audit: same Heisenberg0/1/0 Request Changes
for the current success candidate, not a new Rust/test regression. Exact
source evidence is in the existing result and
/tmp/mini-vpn-p3-completion-scope.jh3pDE/audit.json; all388 Rust hashes unchanged,
actual CurrentOnly baseline8/8. Unbound TerminalGraceExpired invokes original
Target retirement and immediately retries a completion, so prebind expiry does
not leave the proposed original HalfClose pending with released Z. The ordered
C/output page can admit ACK before its later terminal action; the missing
legitimate Bound expiry entry, not a blanket first-ACK prohibition, is the gap.

The user's latest explicit confirmation grants this finite scope; exact
design/write freeze below still precedes its production implementation:
deepen the original CurrentOnly/C-output TerminalGrace normal-consumer Module.
Retain the actual opaque terminal action and original same-clock start/window;
select a positive grace policy and immutable deadline, never reset on install/
reentry/wake. Bound timer registration, retained due and indexed selection to
the original Z capacity, distinct from F; charge allocation, duplicate checks,
expiry/Target operations and publisher retirement rather than hiding them
behind existing K. Preflight all admission failure before owning transfer.
Keep due while Page/lifecycle/continuation excludes ordinary work. The whole
owning expiry must preserve original reducer observations/errors and wake the
original Completion candidate; a committed expiry is not an unconsumed timer.
Exact rejection/pressure/cancellation/unknown/shutdown dispositions and successor
terminal custody belong to this same Module, not a test finalizer or event bus.

Design exit before writing: name actual producer, unique owner and call route;
same-clock positive configuration/original start/deadline; complete disposition
table; checked cold/steady capacity and bounded readiness/retirement costs;
exact existing/new paths/symbols/limits in this write table; same Reviewer.
Next falsifiable vertical is real deadline->Z release->original carrying
Completion->C ACK->actual FIFO->retained successor terminal. Preserve the
existing guards, Source/capability lineage and every later P3 acceptance item.
No replacement, Legless, other lifecycle implementation, shared-fixture/getter,
directory-wide grant or automatic new path is requested. Do not replace this
success path with count-only rejection or mask-only tests. User authority now
opens this TerminalGrace consumer; its exact owning Interface and write set
still require the design exit above. No additional sidecar is needed.

The independent JoinReady alternative audit is complete, same Heisenberg
01a09e36-295a-72b0-ae23-fa0d7ae68e94 (supplement01a09e38-b8c3-7bd0-b948-f16a987ad531),
0/0/1 preserving the existing cause-loss finding. No unchanged TargetIo Adapter
can emit operational Failed readiness, so there is no lawful current tracer.
The latest user confirmation also grants the separate existing-path exception:
owner_target/tests/r7_nested_abort.rs746 may add only single-use operational
failure/typed readiness error on actual selected Join interest and necessary
call counts; unconfigured delegation, other injection behavior and visibility
stay unchanged. Existing bound_failure/completion.rs596 may add that exact
test axis using original Bound/terminal custody, staying below800. No new path.
Only original TerminalFlowSettlement/service_ready_retirement/final retirement
refusal symbols need production repair: retain distinct first join-readiness
cause without replacing Failed-work cause, repeating RESET, joining after the
failure, or releasing original Source/Target/F. Actual target.join failure is
not equivalent evidence. This is now granted to precede TerminalGrace;
it does not open lifecycle policy or waive remaining P3 work. No Rust/test run
occurred during that audit; its388 hashes identify the pre-repair checkpoint.

Executable JoinReady freeze is approved by same Heisenberg
01a09e87-8e41-7b60-a1d1-a54bbeb04361 at0/0/0 (plan only). Exact production files
are owner_target/completion.rs and its retirement.rs child; test files are the
two existing paths above. Replace the original readiness bool with private
Awaiting/Ready/OperationalFailure(TargetFailure)/InvariantFailure(TargetIoError).
Move the exact returned cause into the same terminal before returning the
existing generic block, and refuse later poll/join/release before mutation.
Do not overwrite the distinct Failed-work cause or require Ready on every
successful final-retirement path. No getter/visibility/parent-anchor change.
Runtime evidence uses actual graceful buffered Target custody, original Bound
selection/publication and one-shot Adapter result; same still-valid selection
must refuse re-entry without repoll/join, before any fatal/quarantine finalizer.
Source review proves private typed-cause retention; tests do not directly read
that private field. One exact tracer, both fault variants, then related/full
Target/physical regressions, scoped fmt/provenance/growth/check and same review.
The fixed shared fixture is unchanged; only this test instance disables its
old optional Write fault to build a real successful DATA/CLOSE/EOF/final-ACK
backlog. Exact scope/normal line limits and full P3 obligations stay unchanged.

TerminalGrace design input, 2026-09-14 (factual audit, not a Rust freeze):
same Heisenberg bounded exploration, finalized after submission
01a09ea7-faf3-7b90-a675-adb897aebaa8, confirms the existing original observation
is the correct starting point. Supervisor consume_internal_receipts already
sees full FlowFinished/TerminalGrace, but ReducerFlowDirectionTerminal drops
LocalFlow/tombstone identity to scalar metadata. Enrich this original fact and
preserve the non-Clone Owner wrapper rather than invent a parallel start event.
Main's consumer search also finds ClientFlowDirectionTerminal: preserve its
existing scalar observer behavior; do not silently alter client semantics.

Nested Open/HalfClose completion and observed resume share the actual reduction
and observation-merge chain. A newly generated terminal in a resume belongs to
that production turn; a previously generated FlowFinished merely executed in
a later page must retain its first window. PendingOwnerContinuation currently
has only resume and Actor-arrival/Local origin, while continue_effects_inner
samples now again. Neither that later sample nor output installation is an
original terminal start. Keep one Z-bounded owning lifecycle entry pairing the
early original fact with the delayed original opaque action, not two registries.

The complete packet must also cover Supervisor's committed-consumer failure:
consume_after_model_commit_observed poisons on an internal receipt error before
an observed page may return. Success-page-only registration cannot prove
original fact/time custody there or on unwind. This rules out implementing
only a normal output timestamp and calling the error path complete.

The next concrete freeze must select the exact precommit time point/positive
policy, retain that original sample through committed errors, specify real
timer registration/Output-excluded due service and complete observed expiry,
and name bounded costs and exact files. The earliest distinguishing tracer
uses real effect-budget deferral: original fact before delayed FlowFinished,
same-clock advance, then resume/install without deadline reset or duplicate
registration. It is followed within this same complete consumer by actual
deadline->Z release->carrying Completion->C ACK/FIFO->successor terminal.
No isolated timing tracer discharges the complete path. All388 Rust hashes
still match the accepted JoinReady checkpoint; this audit ran no tests and
did not authorize a proposed Interface or new file. No new sidecar is required.

#### TerminalGrace complete-consumer execution contract (revision 2, design accepted)

Implementation exception, 2026-09-14: the earlier revision2 approval
exposed same Heisenberg Interface review01a09f01-2fc7-72d3-a290-09587c618c1b
(restated01a09f05-0635-7a12-aeec-00684f80cd5a), P0/P1/P2=0/1/0 Request Changes.
The proposed `into_terminal_fact` consumes only the bundle; C previously could
not cross the private Owner/Reducer wrappers for exact install/expiry. Four
scalars cannot reconstruct the original tombstone. User explicitly authorized
the narrow exception on2026-09-14 ("确定授权"):
each original wrapper may expose `pub(super) fn terminal(&self) -> &TerminalGrace`.
Fields stay private; Owner fact stays non-Clone and root-owned through Installed/
Expiring/unknown. No consuming-token conversion, second registry or callback.
This is the sole required narrow exception to the literal getter prohibition;
it is now granted, not an open permission question. Compiled owning operations
and SOURCE review must still prove closure; this grant is not Rust acceptance.
Local Interface executability was8/10 before this correction, not a full P3 score.
The accepted test-only descendant route below needs no production accessor.

The authorized borrowed-token seam is now locally implemented and SOURCE-approved
0/0/0, same Heisenberg01a09f27-d218-7f62-b306-303226a7def1, with header-only
supplement01a09f2a-7658-7130-a511-2f679fb8cdbe. Original Reducer fact holds the
complete token plus generation; both wrappers borrow it and preserve the four
scalar methods/private fields/non-Clone Owner. A real Executor fact survives
delayed output and pairs by full equality. Current393-path evidence:
`/tmp/mini-vpn-terminal-fact.gB8Hna/accepted.json`; four files5439/167/8841/406,
no new paths or ratchet edit. The borrowed-token P1 closes at local Interface9/10,
not full P3. Receipt-release ordering is unchanged: the owning journal/unknown,
consuming bundle transfer, C original-time/ledger and expiry duties below remain.

Independent already-granted time-preflight work may proceed without that token
exception: only session_clock.rs and the approved C tests/terminal_grace.rs leaf
implement/test one deadline_window(Duration) operation. It samples the actual
clock once, validates the positive exact interval and both additions, and
returns value-only (observed SimTime, deadline SimTime, Tokio Instant). It has
no terminal/entry/work capability, per-call allocation or fresh clock source.
It is a mandatory time-admission primitive for the eventual C turn, not a
substitute consumer or a claim of a production C caller. One focused RED/GREEN
at a time covers same-sample values, zero and overflow; source scope, both
feature modes, shared clock tests, provenance/growth/check and same SOURCE
review close only this prerequisite. Token pairing/expiry follows the original
accepted sequence now that the narrow borrowing exception is authorized.
This primitive is now locally accepted: clock100/test282, same SOURCE
01a09f13-9975-70d2-ace3-2422688015c0 at0/0/0. Missing-operation RED/GREEN and
zero-window runtime RED/typed-error GREEN precede four boundary characterizations.
Exact preceding393 hashes/gates: `/tmp/mini-vpn-terminal-deadline.G56H7j/accepted.json`.
It has no production C caller, stores no policy/window and owns no terminal.
Do not count it as original terminal-time or timer/expiry acceptance.

This consolidates the preceding findings into one implementation decision,
not a new sidecar or a request to narrow full P3. Same Heisenberg revision2
review01a09ec9-248c-7ac1-8c2b-93b2ea5c7eac and owning-transfer supplement
01a09ecb-6c02-75b0-b3dd-a415bebd6582 accept the design at0/0/0. This permits
the characterized Move-first implementation sequence under the existing grant;
it does not claim compiled interfaces, implemented behavior or P3 acceptance.

Time semantics: select the original serialized Owner production-turn admission
sample, before its first mutation/Target operation and without an intervening
await. A newly committed terminal from that bounded turn uses that sample;
an old FlowFinished executed in a continuation keeps its first sample. The
grace interval is therefore measured from the admitted producing turn, not
from completion of its possibly delayed output. This definition is explicit,
not a claim to measure the internal reducer instruction's physical instant.
Use the original C SessionMonotonicClock and a required positive, representable
grace Duration in checked construction; no production default. Preflight both
SimTime and Tokio absolute-deadline addition before entering the turn. Keep
the prepared window root-owned until the actual outcome is durably classified.

The exact configuration route is an explicit `terminal_grace: Duration` field
on OwnerControllerPreparation, its sixth new(...) argument, and a fifth
OwnerSessionController::new_checked argument. Constructor failure retains this
same supplied value with the original clock/executor/plan/ready reservation;
into_parts returns it and the parent reconstructs the original preparation.
Validate nonzero duration, exact u64-nanosecond representation and a checked
same-clock absolute window before ready binding. Retain the validated duration
in the C ledger, not in a second parent policy. All subsequent windows use the
same immutable policy. No default or builder fallback is introduced.
The generic C storage has a private Bound(ledger) state and a cfg(test)-only
LegacyUntracked state solely for the already-distinct legacy capacity type;
production has no Untracked variant or conversion into the checked alias.
Legacy fixtures remain legacy evidence, not evidence for this timed consumer.

Keep the original observation chain. Enrich ReducerFlowDirectionTerminal with
the original TerminalGrace while preserving its four existing scalar methods
and Client wrapper behavior. Its non-Clone Owner wrapper remains the original
fact, not a second event. Before any fallible receipt consumption, Supervisor
must own the committed effect batch/current item/observations in its existing
authority. Before Target callbacks, Executor must similarly own the actual
observation bundle. Known errors return/retain these actual values; unknown
unwind keeps the in-flight journal nonzero and blocks reentry. Do not return
only a poisoned flag while losing the original terminal. Beyond the explicit
borrowed-token exception above, no observer getter, owning-wrapper clone,
new callback or second error authority is permitted.
The original C error/aborted ledgers continue to own the returned exact error.
The necessary consuming operation is explicitly
OwnerTargetReducerObservations::into_terminal_fact(self) ->
Option<OwnerTargetFlowDirectionTerminal>. C's header owns the entire original
bundle during all fallible progress and ledger identity/capacity checks. Only
after those succeed may it consume the bundle and infallibly move the original
non-Clone terminal wrapper into the reserved entry. Failure retains the whole
bundle; do not take the terminal early. The first C success-time component now
implements this consuming method; full owning journals are still outstanding.

Accepted partial implementation: normal returned-page custody reuses original
C.aborted_turn before fallible checks. User explicitly authorized only the
existing Knife function controller_aborted_pages_use_one_checked_constructor_and_complete_cold_inventory
to adapt constructor_calls/checked_slot_installations4->3 and quantity wording.
That exact three-line correction closes the stale count without a dummy
constructor, scanner change or weaker literal/constructor/inventory guard.
Same Heisenberg final SOURCE01a09f7a-f3cc-70e1-a459-cfb72cef2c7d approves0/0/0;
real unwind and related runtime suites plus all remaining scoped gates pass.
Current evidence: `/tmp/mini-vpn-terminal-c-journal.aT5m5e/accepted.json`.
The subsequent normal-page ACK interruption and real Bound Target-error
characterizations remain test-only evidence. Latest same Heisenberg
SOURCE01a09f91-e322-7430-b745-92cd46eba849 plus evidence update01a09f94-6d1f-71c2-be8a-8c96cc7905d6,
0/0/0, checkpoint `/tmp/mini-vpn-terminal-error-source.HR1f5q/accepted.json`.
The subsequent C returned-error journal is now locally accepted: same Heisenberg
SOURCE01a0a2c2-1c2b-7270-b9d5-c73486fe4a13, restated
01a0a2c7-fabf-7213-ab1b-742b6c3f237f,0/0/0 Approve, local9/10.
Evidence: `/tmp/mini-vpn-terminal-error-custody.fALxW9/accepted.json`.
User authorized the precise field-list supplement above; Knife only adds
retained_target_errors and adjusts its boundary separator to preserve2624.
All original3/3/0 and inventory guards survive exact inverse hashing.
C's same checked aborted-page slot now owns original Option<OwnerTargetError>
before snapshot, observation transfer and fallible progress. Original bundle
is root-held; the actual phase stays Failed while a local candidate settles.
Unknown retains error/bundle/window and refuses reentry. Known progress failure
returns the original TargetAndProgress with the original bundle retained;
known completion returns the original error and preserves old phase/window
semantics. Actual error-owner count participates in cold exact-zero and actual
generic C Layout accounts for the larger inline header. No second backing.
Real runtime RED->GREEN/ten repeats and known-error cleanup pass; related suites
in both modes, Knife/repository, both growth, scoped fmt/diff and library checks
pass. Four Rust paths3268/688/786/2624 differ;389 unchanged, no inventory edit.
At that checkpoint TargetAndProgress was SOURCE-only; section51 now records
separately accepted direct combined-error fault injection, not natural reachability.
This does not close detailed nested payloads, recovery interruption, original
Supervisor/Executor journals, physical zero or full expiry. Revision2 consumer
scope remains granted. The subsequent Supervisor receipt-journal component is
SOURCE-approved0/0/0, Heisenberg01a0a2d9-2ac3-7030-909f-d8957bbc3ccc/local9/10.
Checkpoint: /tmp/mini-vpn-supervisor-receipt.g2BFGs/accepted.json.
Original Vec/current/observations stay root-held through all receipt/ACK-derived
and catch-up work; first error preserves current/suffix and forbids replay.
One success take, no second K backing; old candidates allocations stay explicit.
Real Bound RESET runtime RED->GREEN/ten repeats and scoped both-mode gates pass.
The later test-only characterization closes actual filtered-ACK-prefix and
permit-callback evidence: same SOURCE01a0a2e9-6348-7bd3-aff8-245c1d748eb4,0/0/0.
Checkpoint /tmp/mini-vpn-receipt-suffix.5optQG/accepted.json; existing leaf446
only,392 unchanged. Both direct-GREEN, ten repeats each and scoped gates pass.
Unbound receipt custody is not Bound/Root or complete DATA/Executor acceptance.
The later authorized complete Supervisor DATA journal is now locally accepted:
same Heisenberg SOURCE01a0a329-6b89-71b2-bbfb-6f72b6f368fb,0/0/0/local9/10.
Checkpoint /tmp/mini-vpn-owner-data-journal.Rb7JPH/accepted.json records five
paths2828/5439/422/217/669,388 unchanged and exact executed gates. One restricted
TCP borrowed bind shares the original by-value validation/commit; it preserves
the original receipt on known Err without changing Client/legacy consumption.
Original DATA Vec/current/fact enters the existing Supervisor journal before
binding/publication. Original SinkAccepted frontier, receipt/ACK-derived fields
and catch-up settle before the sole success take; no second K backing. Actual
DATA RED->GREEN, known rejection and Target4 acceptance custody plus both-mode
related/provenance/growth/scoped checks pass. Existing scan/allocation costs stay.
Executor page/current/conflict-bundle, recovery and full expiry remain mandatory.

Unique normal owner: a private C terminal-grace ledger, inside the same
Controller/output/CurrentOnly tree, owns at most Z entries. The prepared turn
window and bounded in-flight observation journals are explicit transient
custody, not another persistent Z registry. Entry phases are Reserved,
AwaitingOutput(original fact/window), Installed(original fact/window/action),
Due, Expiring and CommittedOrUnknown(original retained outcome). Pair the
delayed opaque action by the complete original terminal identity, including
LocalFlow/tombstone; do not synthesize it from kind(). A committed terminal is
never registered twice. Old untimed live Z at checked construction must be
rejected/returned unchanged; already-expired cold Retiring work needs no timer.

Prepare Z backing/free positions/deadline index before ready binding. Ordinary
turns retain one bounded reservation or the proven-full state; a full model Z
still permits legal nonterminal work and its normal carrying terminal-pressure
return. Do not globally block DATA merely because no new tombstone fits.
Prove one terminal birth per ordinary observed turn; an unexpected second
fact retains both real facts and quarantines, never overwrites the first.

The actual physical timer Adapter is one retained Sleep registration per
CurrentOnly, prepared before final publication; no task/spawn or per-terminal
future. It points only to the C ledger's earliest immutable deadline and
publishes through the existing controlled Output wake. No work capability
escapes as a deadline getter. A complete C/output operation borrows this
registration to install/poll/expire under the original publication guard.
Timer-ready is a hint; the same clock/deadline comparison decides actual due.
Keep due through Page/continuation/lifecycle exclusions. Pending excludes
ineligible work without live-spinning or resetting/rebuilding the deadline.

The registration's concrete control states are Dormant, Armed(head identity /
immutable deadline), and DueHeld(head identity). The C entry owns the work;
these states own only the one timer registration and its correlation. Poll a
future Armed Sleep once per selected timer unit. Pending clears its current
request and relies on the registered controlled Waker. Ready transitions to
DueHeld before returning: never poll that completed Sleep again. DueHeld with
an uninstalled action or ordinary-input exclusion requests no timer work;
it does not use Hint merely to keep Output runnable. DueHeld with an Installed
entry and legal ordinary admission requests one expiry unit. A new earliest
deadline rearms only after the previous owning entry is settled; empty is
Dormant. Compare now before arming a future head, so an already-due successor
enters DueHeld without depending on a fresh timer wake.

Every successful selected visit recomputes requests from the actual C/output
state before finish. A pending TerminalGrace action requests an installation
unit even while that same lifecycle port excludes ordinary input. Other
unsupported lifecycle kinds remain retained and ineligible. Installation,
Page-to-Drained/Vacant transition and continuation completion each recompute
whether retained DueHeld has become executable and keep CONTINUATION runnable
through the existing returned runnable mask. Output flush Registered remains
asleep until the actual Actor capacity/delivery edge; its successful visit
requests the original pending page, whose later admission/installation then
re-enables due. Thus finish_continuation may clear the consumed revision but
cannot strand retained executable work: the next selector also receives the
recomputed Output request. No manual wake, synthetic revision, new publisher
or polling of an already-completed Sleep substitutes for this chain.

Opaque pairing uses one closed extension of the existing lifecycle port:
install_exact_terminal(expected: TerminalGrace, installed: &mut
OwnerOutputLifecycleInstalledSlot). It compares the private action's complete
TerminalGrace equality, including LocalFlow and tombstone, before the existing
rollback-guarded install. It returns a typed rejection without changing either
owner, or moves the same action into the supplied root-held slot. C chooses the
entry using existing scalar kind() only as an index hint, then supplies that
entry's original full fact. The operation exposes no token getter or callback.
C holds an Installing phase and the actual slot before invoking it; unknown
interruption cannot lose the port action or an already-installed action. The
slot remains in that entry through expiry; only a settled actual observed
expiry may release the complete entry. No second physical Z registry is used.

Output service fairly alternates its actual eligible frame/flush, lifecycle
installation and timer operation. Each selected visit performs only one such
unit (an existing E-bounded effect page remains one separately charged unit).
Do not weaken the original ordinary-input exclusions. The installed action
fills its already-correlated entry, so it does not need a second Z allocation.
When ordinary admission is legal, move the due entry to Expiring before the
existing full apply_event_observed(TerminalGraceExpired) path. Preserve Target
retirement, reducer observations, indexed Completion-capacity handoff and
original C/output admission. A precommit carrying rejection restores the same
due entry/window/action. Any committed or unknown outcome stays retained;
it cannot be restored as an unconsumed timer. Success releases the old entry
only after actual outcome ownership is settled; later selected Completion may
then produce its own original ACK and successor terminal entry.

Cancellation drops only the borrow/observer, never the entry/registration.
Shutdown stops new ordinary admission and keeps all unsettled entries, journals,
actual outcomes and registration in original root custody; no stop-discard or
actor-zero claim substitutes for full lifecycle settlement. Foreign/stale
terminal, clock failure, overflow, allocation pressure and postcommit failure
have exact return or sticky owning quarantine. Replacement/Legless/session
supersession are not newly implemented; they remain excluded with custody.

Concrete bounded backing: one cold-reserved Vec<TerminalSlot> of exactly Z
initialized slots. A slot stores phase/payload, one free-or-birth-order link,
and intrusive AVL left/right/height metadata keyed by SessionFlowId. The fixed
session identity and full TerminalGrace equality validate the key on access.
Free/head/tail/root are indices, not owning capabilities. Birth-order links
also order deadlines: one immutable positive policy and nondecreasing same-
clock producing-turn samples imply nondecreasing deadlines; unknown/rejected
turns cannot insert a late old birth after newer admitted work. Reject a broken
ordering as retained invariant failure, not a sorting repair. Insert/remove/
pair use O(log Z) identity work, head/deadline and free-position work are O(1).
No new O(Z) duplicate scan, per-birth allocation or second deadline array.

The transient storage is one C Prepared/Entered/ReturnedOrUnknown turn header,
one original Supervisor receipt journal and one original Executor observed
journal. Their vectors are moved original effect/output backing, not cloned
batches. A Supervisor journal owns remaining batch/current item and original
observations; an Executor journal owns existing page/current work and actual
observations, including a conflicting incoming bundle until quarantine.
Keep original existing effect/output bounds: K pending work, E output page and
the existing incremental burst of3; conservatively charge both the existing
K+3 transient work peak and E outputs during transfer, even when backing can
be moved. No second K queue is added. One extra original observation bundle
is the conflict reserve, never an overwrite or unbounded history.

Cold Layout composition is checked using the actual Rust types: old resident
layout plus TerminalSlot[Z], the two inline journal enums/turn header, and one
Pin<Box<Sleep>> allocation (Sleep payload as well as its pointer/control state).
Charge the larger simultaneous old/new owner representation during transfer
and the original K+3 work/E output backing; count it once when the same backing
moves. Fallible backing reservation and timer preparation precede binding or
final CurrentOnly publication respectively. Any additional allocation, actual
capacity beyond the checked reserved backing, or journal population not covered
by these exact bounds is a write-freeze failure, not an implicit new allowance.

The old complete expiry is not logarithmic. Let M(F) be one actual model
snapshot (F flow inspections), S(F,R,Q) one Supervisor snapshot (M plus R live
replay-extent and Q quarantined-extent inspections), and X(F,R,Q) one Executor
snapshot (S plus its three F scans). The normal Bound expiry body charges
2*M + S + X, the original Target tombstone operation/lookup, two progress
removals in 2*(F+Z), one indexed Completion-capacity handoff, and the new bounded
ledger/timer work. The 2*M are receipt-consumption and catch-up invalidation;
S supplies flow_retired generation; X is C's committed-generation check.
Original actor/C/output preflight costs are additionally charged, not included
in those four snapshots. E-page reducer/replay work and encoded bytes retain
their separate original charges. R/Q use original replay/quarantine capacities;
do not silently substitute F or optimize snapshots by adding a new getter.

For scheduling, retain the four actual process bounds with class capacities
C_i=[N+1,N,N,N], budgets B_i>0: rounds_i=ceil(C_i/min(B_i,C_i))+1 and
quanta_i=rounds_i*sum(B_i). Let Q_session be its existing SessionCommands
quanta bound. A continuously eligible one-unit lifecycle operation has a
conservative bound 3*13*5*Q_session: three eligible Output sub-operations,
thirteen categories, five sources, then the real process service bound.
Serving at most Z already-installed due entries costs at most Z such units;
installation and initial timer observation are separately charged units.
All sums/products, including old 13*F*5 and (3*F+2)*13*5 bounds with the new
factor3, are checked on cold inputs. Excluded time awaiting a genuine external
capacity edge is not a wall-clock guarantee. After that edge the real request
chain above must resume without user wake. This is a scheduling-unit bound,
not an assertion that Target calls or E pages take constant time. Rust Layout
and exact call-count tests must challenge these selected formulas before
acceptance; an uncovered operation stops promotion, not the full-consumer goal.

Exact implementation locations selected for review: Supervisor root retains
declaration/storage anchors; its complete postcommit receipt-consumption bodies
move to new private supervisor/owner_observation.rs. Executor likewise retains
anchors; its complete observed reduction/page/error-retention bodies move to
new private owner_target/observed_turn.rs. C keeps anchors and moves complete
observed-turn/timing/terminal-ledger Implementation to new private
owner_session_controller/terminal_grace.rs. Their existing private operations
keep equivalent ancestor scope, no re-export or pass-through. Owner output
keeps its public declaration anchors and moves complete lifecycle impls/guards
to private owner_output_drain/lifecycle.rs before adding the owning timed
installation operation. Every Move has byte-exact characterization/provenance
and separate GREEN-to-GREEN evidence before behavior changes.

Remaining existing production edits are confined to C construction.rs and
ready_service.rs; owner_target service.rs/completion.rs for original journal
integration; session_clock.rs for one checked same-sample deadline operation;
physical_session_root.rs/service.rs and continuation.rs for actual registration,
selected Output/Completion service and inventory; server_root.rs/construction.rs
and owner_session_controller/capacity.rs only where the real input/checked costs
require it. Naming a nonexistent path or needing another behavior axis stops
the write freeze; this is not a directory grant. No DATA hot-path allocation,
new dyn dispatch, global root lookup, worker or callback is permitted.

Checked-constructor migrations preserve old tests' original semantics and
failure-return assertions, adding only explicit positive test policy: existing
owner_session_controller/tests/capacity.rs; owner/continuation/tests/mod.rs;
physical_session_root/tests/current_only.rs and its existing bound_source.rs
child (both OwnerControllerPreparation::new calls, parameter migration only);
actor tests r7_physical_attach_permit,
r7_initial_acceptance_settlement plus its initial_composition child, and
r7_owner_physical_recovery. Existing oversized parents may not grow; necessary
pure Moves must be separately characterized and named before writing. New
timing and complete-path tests live in private C tests/terminal_grace.rs and
physical current_only/terminal_grace.rs, each at most800. Same Heisenberg
route clarification01a09f03-183a-7d80-bcee-d4f6e84b08e3 is0/0/0: the C timing
leaf has exactly one `#[cfg(test)] #[path = "tests/terminal_grace.rs"] mod tests;`
declaration in C's production terminal_grace.rs child, not a sibling declaration
in C/tests/mod.rs. Its logical route is terminal_grace::tests, giving descendant
access to original private ledger/window state without a production getter or
visible fields. The leaf constructs its real fixture privately; sibling test
helpers are not exported. The physical leaf retains its private parent route.
No existing test implementation is weakened or replaced.

Execution order: required characterized Moves and Integrator shrink ratchets;
one actual deferred-FlowFinished timing RED/GREEN; then the real expiry->Z
release->original carrying Completion->C ACK/FIFO->successor path RED/GREEN.
Follow with independent pressure/clock-overflow/unknown/cancel/output-excluded
due tracers, both feature-mode related suites, shared Client/Supervisor tests,
provenance, scoped fmt/diff/check, both growth routes and same SOURCE review.
The output-excluded tracer must let the real deadline pass during actual FIFO
pressure, release only the real pressure, then observe one install/expiry and
park without manual wake. Both unchanged Bound-source scenarios must pass with
their explicit policy; zero/overflow policy must return original construction
inputs before binding. Existing Bound assertions/fixture/tail remain intact.
Do not declare the consumer accepted after only the timing tracer. No external,
commit/push, P4/P5/P6, full P3 or whole-root zero claim is authorized here.

First preparation Move is locally accepted: the four Supervisor postcommit
methods alone moved to owner_observation.rs, parent5597->5439/child169 and exact
Integrator ratchet. Same SOURCE01a09ed0-eeaa-7123-8765-f7b443925f80 returns0/0/0.
Supervisor48 before/after and both-mode/shared related/provenance/growth/check
gates are green; latest result and
`/tmp/mini-vpn-terminal-grace-move.jjJY2a/accepted.json` own exact evidence.
No journal, policy, timer or consumer behavior has been added. Continue the
remaining characterized Executor/C/output Moves, then the accepted tracers.

Executor Move exact scope: owner_target.rs and private observed_turn.rs move
the21 reduction/resume/page/error-retention methods as one implementation;
data/storage, raw I/O bodies and admission anchors remain at their old seams.
The current user-authorized in-scope self-repair also migrates only the first
source guard in existing owner_target/tests/r7_owner_ack.rs: child mint/consume
counts stay exactly1, new parent absence checks require0, and the original
parent-private gate/Supervisor gate assertions stay unchanged. All behavioral
tests and callers are unchanged. This necessary source-routing repair does
not authorize a new production axis, parser or weaker provenance assertion.

Executor preparation is now locally accepted: parent8841/child754, source
guard403 and exact Integrator ratchet. Same SOURCE01a09edd-b03a-7121-a9c0-1b02784619bb
returns0/0/0; exact four-block/full-parent inverse and guard/inventory inverse
hold. OwnerTarget148 both modes, shared/related/provenance/growth/check gates
are green. Latest result and
`/tmp/mini-vpn-terminal-grace-executor-move.vUBx4S/accepted.json` own evidence.
Only C/output preparation Moves remain before the accepted behavior tracers.

C Move freeze: seven complete observed-page/progress/error methods move to
the selected terminal_grace.rs; storage/admission/callers remain. Existing
Knife aborted-page guard visits both parent and child with its same AST audit
and exact four construction/install assertions; only local blank separators
offset those route lines, keeping oversized Knife2624. Existing C capacity
source guard also scans the child, without changing behavioral assertions.
This route maintenance adds no parser, behavior axis or new test Module.

C Move is accepted: exact seven-body/full-parent inverse; parent3268/child280,
Knife2624 and capacity guard474; SOURCE01a09ee9-9f43-7502-9641-836d458823c4
0/0/0. C44 both modes and related/provenance/growth/check gates green. Exact
evidence: /tmp/mini-vpn-terminal-grace-controller-move.PKuQHU/accepted.json.
Output preparation alone remains before the accepted behavior tracers.

Final output Move freeze: only owner_output_drain.rs and new private/default/
unconditional lifecycle.rs; fourteen complete lifecycle impl blocks including
the original rollback Drop, with declarations/storage/FIFO I/O left parent.
Original parent-private methods retain equivalent pub(super) scope; existing
pub(crate), cfg(test), trait signatures and callers remain unchanged. No new
callback, test implementation, installer behavior, state or policy. Baseline
output17 is green; predicted parent990/child698. Only Integrator removes the
1676 grandfather entry after verified below-limit shrink, then final gates/
same SOURCE review. Prior391-path checkpoint must remain otherwise unchanged.

Final output Move is accepted: SOURCE01a09ef1-1e7b-7893-8f53-e491d93a0be2
0/0/0 verifies14 complete impl inverses/full-parent equality; parent990/child698,
obsolete1676 inventory entry removed, governance751. Output17 both modes and
related/provenance/growth/check gates pass; current392-path evidence is
/tmp/mini-vpn-terminal-grace-output-move.YMWwGL/accepted.json. All four required
preparatory Moves are complete. No timer/journal/policy behavior yet; proceed
directly to the accepted deferred original-fact timing tracer and complete
expiry/Completion/C-output/successor path, not another prerequisite sidecar.

Executable remaining checklist for the current P3 exit (refreshed2026-09-15):

This is the one current execution checklist. Earlier candidate/RED passages
in this joint contract are historical predictions; latest stage results and
the following checkpoint supersede their implementation-status claims:
`/tmp/mini-vpn-terminal-consumer.gnAZqG/c-inactive-pressure.json`.
No acceptance obligation is removed. A checked row accepts only its named
scope, not the entire consumer or P3.

- [x] Required four pure Moves, checked same-clock policy, original C returned
  page/error ownership, Supervisor receipt/DATA and original Executor journals.
  Prior exact evidence remains linked in the current result; no repeat Move.
- [x] Actual first success route: original terminal/action -> C installer ->
  one physical Sleep -> original expiry -> indexed carrying Completion ->
  C output/FIFO -> peer's unique final ACK -> successor expiry.
  Prior full chain10/10 EACH mode and same SOURCE0/0/0:
  terminal-expiry-draft.json in the checkpoint directory. This is success
  evidence, NOT complete consumer acceptance or global-fmt/full-library proof.
- [x] Narrow shutdown observer cancellation: actual blocked join is observed
  before dropping its future; same root Joining and actual C/timer remain held.
  Actor-only reentry explicitly remains nonzero for the whole root.
  terminal-cancel-characterization.json, same SOURCE0/0/0.
- [x] Normal multi-Z index lifecycle: real C/MemoryTarget/output/lifecycle/Sleep
  ports; four insertion orders, actual expiry/removal and same-slot reuse;
  readonly AVL/FIFO/free partition checks. At that checkpoint child284/parent797;
  exact direct GREEN plus10 repeats EACH mode; same SOURCE0/0/0.
  c-expiry-index.json. Not Active Root, capacity exhaustion or FIFO pressure.
- [x] Required matrix1 subcase: genuine BoundInactive HalfClose pressure and
  original expiry retention. Real EOF/final ACK prebind, A RESET occupying Z1,
  B CLOSE/original continuation; HalfClose once, completion1 retained after
  actual A expiry. Same SOURCE0/0/0; exact1/1 plus10 repeats EACH mode.
  c-inactive-pressure.json; only existing child522 changed, parent797 unchanged.
  Model/Target tombstone and C fact zero are not Executor Flow/source zero.
- [x] Matrix1 generic single-waiter/same-resource duplicate registration and
  selected RESET retry are already exercised by the existing actual component
  test. Three real competitor RESETs reoccupy Z; inner and outer original paths
  both register the same waiter, exact ReturnedReset/transit/membership survive,
  next handoff works and final output occurs once. Exact1/1 plus10 repeats EACH mode;
  same SOURCE0/0/0, existing-waiter-coverage.json. No new Rust test needed.
- [x] Matrix1 HalfClose-specific repeated-pressure payload/association.
  User-granted existing component completion.rs268->640; old268-line prefix
  exact,395 other Rust paths unchanged. Real prebind EOF/finalACK and actual
  install/activate; three competitorZ rejections with legal selected retries
  preserve full SinkHalfClose incl completion_id and Source/Driver LocalFlow,
  association/position/generation. Each retry starts wait=None, rejection
  restores Some; transit0/HalfClose1; unique final ACK/original B terminal.
  half-close-pressure.json: first exact GREEN plus10 EACH mode, related/growth/
  scoped gates pass; same Heisenberg SOURCE01a0a467-ae9d-7063-b918-94178b816e98
  0/0/0 ONLY this component test. Original outputs held; no production/new path.
  Multi-waiter topology/other-resource limits, cancellation/unknown, physical
  timer and whole-root zero are not inferred; remaining matrix3-5 still apply.
- [x] Current matrix2 output-excluded deadline and genuine pressure relief:
  unchanged real physical fixture, four actual actor ACK permits/fifth FIFO,
  same-clock101ms and selected service retain original C/timer/actor/FIFO;
  release only writer gate -> exact5 nonfinal/1 final ACK, successor expiry/park.
  Parent798/new child232; physical-pressure.json, exact+10 EACH mode and related
  gates pass; same Heisenberg SOURCE0/0/0 ONLY this test. No production change.
  Not internal DueHeld, complete budgets or whole-root shutdown acceptance.
- [x] Required current matrix 3: first-path applicable cancellation, unknown and
  postcommit result ownership. Closure evidence is51H below. Narrow observer
  retention is not full shutdown; new51B topology and P4–7 remain mandatory.
  Direct TargetAndProgress injection duty below is locally closed; owners stay nonzero
  until its real settlement, with no synthetic clean outcome.
  Bounded SOURCE gap map01a0a7f2-d901-7733-99b2-bf259e358cd8:0/1/0 identified
  coverage entry gaps, not confirmed production loss. User granted the four
  exact paths and direct combined-error fault injection is now accepted:
  finish_target_error reuses the existing cfg(test) progress flag after real
  error/bundle custody and preflight; controller docs only/net-zero3268;
  timing parent800 private path; new returned_error.rs140; hook child1088.
  Actual checked RESET/resume-ID exhaustion gives expected Target-only RED,
  then GREEN typed TargetAndProgress, original full terminal/window/Executor
  tail nonzero, no duplicate error owner or publication, two stable rejected
  reentries. No non-test behavior or natural second-failure claim. Exact1/1,
  10 repeats EACH mode/related/provenance/growth/check/scoped checks green;
  same Heisenberg SOURCE01a0a81b-9117-7261-b0d6-edc28a989ead0/0/0 Approve ONLY
  this component. /tmp/mini-vpn-returned-error.405z5b/evidence.json owns evidence.
  The subsequent one-leaf grant is completed: returned_error.rs140->326,
  old combined-error body byte-exact,397 other Rust sources unchanged. Real
  checked admission/arrival/window/RESET yields original error/fact; only the
  existing nested wrapper is injected. Known caller versus unknown C preserve
  Box pointee identities/full terminal/generation/window/reserved vacant slot
  and nonzero Executor tail through two rejected reentries. Normal C facts
  stay empty; this is not recursive inventory, natural conflict or root zero.
  No production hook/route/storage/API. Direct GREEN+10 repeats EACH mode,
  related/provenance/growth/check/scoped checks pass. Same Heisenberg SOURCE
  01a0a98f-df19-76a3-b110-1237761c6eb3:0/0/0 Approve ONLY this component.
  /tmp/mini-vpn-nested-observation.9o5AQA/evidence.json and2026-09-16 local-results
  own the new evidence; prior /tmp files were unavailable, not reconstructed.
  Current recovery LegActivated/Transmit yields no observations/Target callback;
  legacy nested-error exits cannot establish a checked-Bound unwind witness.
  This is not a universal unreachability proof or a waiver: natural nested
  production, recovery interruption, boxed cursor and complete nested inventory
  remain distinct, with no matrix3/P3 closure implied by the defensive test.
  SOURCE01a0a996-27b5-7452-9908-b58799cc1bdf0/0/0 found no new reachable
  unknown-loss defect. Natural AttachedRecoveryQueueBlocked carries the same
  drain back to C Publish. This specific applicable cursor duty is now accepted:
  actual checked bootstrap/nonempty recovery/FIFO1/E1, three pressure returns,
  rejected execute/repeated publish, real acceptance/OpenResult delivery and
  original HandoffReady. Same SOURCE0/0/0; one test leaf450->706, original prefix
  and410 other Rust hashes unchanged. Exact10 repeats EACH mode, fixture9,
  Controller61, Knife190/repo20/growth7/check and scoped gates all pass.
  `/tmp/mini-vpn-p3-recovery.HLxB8L/evidence.json` owns exact commands. Slot frees
  at submission; the test then performs actual peer receipt/wait settlement.
  No new panic hook, Active Root, Box-address identity or complete nested/unknown
  coverage is inferred. Subsequent51F/G/H close current reachable holders and
  applicability; legacy tests do not substitute for checked-Bound evidence.
- [ ] Required current matrix 4: actual generic Layout/cold peak, complete
  call-count charges and capacity boundaries. Only index operations are
  O(log Z); original2*M+S+X/Target/progress/page/replay work still counts.
  The four normal index scenarios do not prove capacity exhaustion or OOM.
  SOURCE01a0a998-7dd5-7573-a6b5-3e3f3dff9ced0/1/0 retains P1: actual-type
  aggregate cold peak or equivalent upstream-bound implication is missing.
  C+Z, binding arrays, CurrentOnly+Sleep and generic resident Layout checks
  are separate; compose by real live phase, count transferred backing once
  and do not add embedded journals twice. No memory-safety/OOM defect is proved;
  timer allocator-abort remains the explicit model. First prove a candidate
  passes earlier plan limits before expecting any aggregate-overflow RED.
  Bounded follow-up: declared K is only nonzero and >= the derived work-DAG
  requirement (Target config67/construction146/C capacity152); E's separate
  encoded-byte bound does not bound K. Therefore an upstream proof must derive
  effective reachable work/backing, not assume declared K+3 is checked. The
  algebraic K=usize::MAX candidate is not a tested whole-constructor witness.
  The separately granted direct-cold test is now locally accepted: existing
  server_root/tests/ready_scheduler/process_plan.rs415->433, old prefix exact,
  no production/API/new path. Actual MemoryTarget resident Layout plus valid
  process arithmetic calls production cold and returns exact Layout/full plan.
  Direct GREEN+10 each mode, both Root25/Ready25/CurrentOnly13, Knife162/repo20/
  growth7/checks/strict standalone/scoped checks pass. Same Heisenberg SOURCE
  01a0ad1d-21b4-7b22-96ec-caba625f586b:0/0/0 Approve ONLY this characterization.
  /tmp/mini-vpn-process-cold.YMBNAt/evidence.json and2026-09-16 local result.
  This fills from_commands-versus-cold coverage only, not aggregate P1.
  SOURCE follow-up01a0ad2a-fa99-7e50-b365-f80a228cac16 retains0/1/0: freeze
  actual capacity/growth/transfer discipline before designing that peak check.
  Logical K/E are not Vec/VecDeque capacities; raw output can hold E+1 with
  NeedsResume and pop does not shrink backing. Conversion/admission scratch
  may coexist with both source and destination. Existing flow::check_layout
  already aggregates its local pieces; the missing relation crosses real
  construction/runtime phases, recorded in the existing local result.
  User CONFIRMED original-owner capacity/reserve/growth control. This resolves
  the direction choice, not the P1. Same consolidated SOURCE
  01a0ad48-5cb8-7ed0-81b7-bf821b6188b3 independently verifies the scope below.
  No repeated direction request, unexplained 2*K multiplier or post-allocation
  capacity check as a substitute for pre-allocation peak proof.
  Required scope: original Reducer effect/recovery/map backing and TCP window
  descriptors/reservation scratch; Supervisor replay/quarantine/receipt scratch;
  Executor queue/current/returns; Output/C raw page and transfer; progress nodes;
  concrete Target retained buffers/maps/addresses/returned buffers; phased Root
  composition. Every domain needs a checkable discipline, not necessarily a
  replacement container. Preserve existing normalized payload contracts and
  Flow/readiness/Z/resident local Layout checks, adding their actual composition.
  Permit bounded on-demand/block preparation; do not densely allocate all K
  merely for audit. Charge rounded blocks, retained free blocks, directory/index
  storage and old/new overlap. Prepare possible generated burst BEFORE reducer
  or Target commit; stage_entered_reduction receives already-committed work.
  Representation decision: keep contiguous Vec/VecDeque backing under explicit
  original-owner growth admission; no new map/ring or directory is prerequisite.
  Same SOURCE01a0ad58-2d62-7b31-80c4-b7701cdaa081 accepts that choice. First complete
  code package spans original producer effects, Supervisor journal, Executor
  current/suffix and raw outputs. Other map/Target/progress duties above remain
  mandatory P1 work, but need not all be rewritten in this first package.
  The latest existing local-result entry records a concrete bounded-doubling
  proposal, stable allocation conditions and actual precommit call boundaries.
  Joint51D now defines the finite requested batch/error/write packet. Same
  SOURCE01a0ad6e-ea3c-7270-8a07-0a0f2da40f62 approves only that scope proposal0/0/0.
  User explicitly grants the complete packet/limited unsafe and continuous P3
  repairs; freeze the actual full fixture/first RED before its test. The real
  checked-constructor -> growth/reuse/rejection tracer remains mandatory;
  helper-only or Executor-only success cannot complete the packet or P1.
  Shared private allocation machinery is only a candidate, not permission for
  unsafe/global allocator/blanket collection changes. Keep original complexity,
  no second K queue or per-record audit allocation. K/E, Client/public contracts,
  exact custody, allocator-abort and all P3 acceptance duties remain unchanged.
- [x] Required current matrix5 shared-component invariant: actual post-suspension
  late wake and independent Flow DATA. Original A/B ReadReady return Pending;
  A's same publisher is invoked only after real RESET/FlowFinished/TerminalSettling
  and publication finish. Legal pre-B dispatch proves parked/no read candidate
  and identical Target snapshot; then B actual read/Driver/inbox/reduction yields
  the complete exact DATA Frame once. Original Source identities, B association/
  position/generation, A TerminalSettling/Z and B unACKed Replay11bytes/1segment
  stay held. RESET's earlier notification cannot satisfy the test.
  Completed user two-path grant: parent commands/tests/bound/flow_lifecycle.rs
  721->722 only private mod; new flow_lifecycle/terminal_read.rs431. Parent inverse
  exact;395 other Rust paths unchanged; no production/getter/wrapper/governance.
  terminal-late-wake.json: final exact+10 EACH mode, related/provenance/growth/
  scoped checks green; same Heisenberg SOURCE01a0a7ec-8cc3-7c51-b560-7077ba7bb844
  0/0/0. Reviewer ran no tests. The row itself does not demand physical injection;
  matrix3/4 and combined consumer/physical acceptance remain open without narrowing.
- [ ] After these rows: full applicable both-mode related/provenance/growth/
  scoped checks and same independent Reviewer consumer closure. Known14 old
  service.rs formatting hunks are not a new debt or a global-fmt PASS.
- [ ] Then the five full-P3 vertical packages in B below: new OPEN/static
  progress; actual TCP/DNS; all process/local budgets and physical capacity;
  Legless/expiry/full association; final full lifecycle matrix/review.
  Each needs its real callable/write scope and falsifier before implementation.
  These are required P3 work, not later-stage waivers.

The user's continuation grants the previously requested private
C tests/terminal_grace/expiry.rs and parent private route. Because that parent
is itself path-routed, compilation requires the explicit relative
`#[path = "terminal_grace/expiry.rs"] mod expiry;`; no visibility expansion.
The previous bare-mod E0583 was feedback-loop failure, not behavior RED.
Existing production scope is unchanged; this is not a directory grant.
P4 table/bootstrap, P5 fused publication, P6 production caller, public cutover,
external/R8-R9 and whole-server zero remain their explicit later stages.

The existing TCP terminal operation bulk-drains and publishes a TUN action;
it is not the required bounded Owner-only revocation. Its needed operation
must be checked against the exact tcp.rs/driver_receive.rs write axes before
any implementation. Independent Mencius audit confirms no eligible operation:
terminal/local-abandon are bulk; factory close affects only new Flow admission.
The user's subsequent explicit authorization grants this exact extension: existing
tcp/driver_receive.rs may own the complete Owner-only terminal-proof and source
admission revocation alongside its selected lane polls. tcp.rs remains unchanged;
new parent storage would require a separate exact grant. Freeze exclusive Owner
Flow/Driver/producer-reservation proof and controlled-wake ordering before TDD.
This resolves only the missing Driver behavior axis, not the three source P1s.
Mencius's child-only audit confirms full LocalFlow plus shared PortState identity,
Owner direction, original factory binding, and same-lock reservation/action
checks can use existing fields. Actual non-escaping Owner producers remain a
call-graph premise: a zero reservation counter alone is not exclusivity proof.
Receiver close does not wake a parked receiver; preserve both selected drain
obligations. Factory progress can synchronously unwind: first retain the real
terminal/work/source, revoke under the admission lock, release that lock, then
publish progress. No bulk dequeue, TUN action or new TCP storage is selected.

The same Heisenberg's bounded executable-choice review returns0/1/0 Request
Changes for the complete C/root terminal path: actual TerminalGraceStarted
remains lifecycle-pending and excludes subsequent ordinary receipt; its normal
installer/clock/deadline/due owner is still missing (section40). He explicitly
permits the following original-Flow component convergence in existing scope;
this is not closure of the three Rust P1s, A3 or P3 and not verified compilation.

The selected component representation replaces the original Flow completion
position with Operational / Failed / TerminalSettling, without a second Flow
or ledger. Operational retains actual local/cold completion origin; Failed
retains exact failed work/cause and Prepared / StagingUnknown / InTransit /
Rejected feedback; TerminalSettling retains actual TerminalGrace/reason, original
work/feedback/source and source revoke/drain phase. The independent single sink
position owns Write(offer,segments) or HalfClose(completion), never duplicated
failed work. Source stays in the original Flow. Runtime RESET reduction does
not release failed work before its actual FlowFinished effect is entered.
Driver-origin rejection retains its charge; actual reduction or matching full-
source terminal disposition is the only ordinary decrement. Cold/local origin
has no Driver charge. Independent DATA/CLOSE cannot settle another RESET.

Component execution order is: (1) characterize and move the entire existing
PendingTargetCompletion impl from owner_target.rs to existing completion.rs;
all three methods' actual callers already live there, no forwarding method or
new visibility is needed; (2) Integrator lowers actual parent shrink; (3) one
real component falsifier at a time for terminal-first late DATA, independent
CLOSE and Z1->1->0, then exclusive Owner revocation/closed-lane drain and selected
completion/join/retry; (4) full source review plus physical evidence. Existing
Bound full-map retry/join cannot become a selected unit. Genuine cold-Retiring
must use existing Retirement service, not ordinary Live Driver selection;
semantic terminal is not a metadata retirement transition. First-A may retain
actual F/source/publishers nonzero but cannot claim reuse or full retirement.

Step1 changes only the exact impl and required imports in owner_target.rs and
owner_target/completion.rs, expected9634->9590 and446->about492. Validate original
body hash, all Owner Target tests, current four lifecycle tests, fmt/diff,
provenance, standalone/Cargo governance and same Reviewer. No behavior change
is mixed into that Move. Later phase/selected service changes use only the
enumerated existing files and leaf; actual limits1500/800 and parent shrink
still apply. Complete physical terminal acceptance additionally requires a
real owning lifecycle installer/due model or a separately reviewed settlement-
only admission amendment. Neither alternative is selected or authorized here;
do not weaken the existing output/lifecycle guard to make components appear
end-to-end complete. This remaining blocker is mandatory P3 work, not a later
stage reclassification.

Step1 execution is accepted only as pure Move SOURCE0/0/0: actual9590/491,
44-line hash identical, parent inverse exact;122 tests before/after, Knife162,
repository17, both library checks, fmt/diff and both governance7/7 pass.
Integrator alone lowered the exact parent ratchet9_634->9_590. No phase behavior
was added; original three P1s and the complete-path lifecycle blocker remain.

These four rows are the design exit, not invitations to create four new design
stages. Reuse the already accepted contracts. The next packet must supply one
concrete representation/callable/write-set and failure prediction for this
route, or expose one genuine unresolved architecture choice for the user. Do
not return to another round of unbounded prerequisite subdivision. Once these
necessary gates and scope pass review, proceed to compile/TDD under the standing
full-P3 execution authority; review success alone is not code evidence.

### B. Later verticals still required before full P3 completion

| Obligation retained in P3 | Why not a first Active DATA prerequisite / required completion |
|---|---|
| New OPEN and static Target progress | Existing genuinely opened Flow can characterize Active DATA. Later implement precommit physical F/O, one model classification, duplicate handling, static begin/poll/finish/open_inventory and removal of the synchronous route; real pending/completion/cancel/fatal/shutdown and reducer-pressure custody required. |
| Concrete TCP/DNS | MemoryTarget is the same static port's local Adapter, not WAN proof. Later audit supported primitives and implement bounded numeric/domain connect, refusal/timeout/late socket and exact socket/resolver/timer inventory, without workers/dyn/blocking resolver. Depends on split-phase progress. |
| Complete process/local fairness and capacity | A3 proves every category/allocation actually used by A, not all future service. Later exercise all four dedicated budgets, local competition, stale generation, retirement/class migration, final checked delay formulas and full physical/cold Layout under all P3 sources. |
| Legless/expiry and full association | Healthy Legless/pre-join work, genuine semantic S/D, candidate-set/no-successor, Installed versus pre-D pressure, bounded normal no-ACK, actual due consumer and initial C/B/history migration remain mandatory for full binding/expiry claims. Their new producers are not introduced by A. Two broad P1s stay open until their own complete vertical closes them. |
| Full lifecycle and acceptance matrix | Any Target failure/RESET/FlowFinished/action actually produced by A must already be owned and settled or explicitly retained by A; this is not deferrable. Later extend to every new source/phase, all amendment counterexamples, regressions/provenance/growth and independent final full-P3 review. No single GREEN or Move checks this row. |

### C. Explicit later stages, not prerequisites or accomplishments of P3

P4 owns the real parent table/bootstrap task/result custody. P5 owns
PreparingParts/fused whole-root adoption and Live publication. P6 connects the
real private Case C caller; P7/deferred final owns the subsequent whole-custody
review, facade/binary and whole-server zero. P3 must preserve their real owning
input/return requirements, but need not implement those objects to run shared
service in a private fixture. No public TUN, VPS/WAN/M2 or performance claim.

### D. Matrix4 effects-storage code packet — locally accepted, not aggregate P1

This is the finite implementation envelope for the representation selected in
SOURCE01a0ad58-2d62-7b31-80c4-b7701cdaa081. Consolidated SOURCE
01a0ad69-8e9a-7790-b4b8-5ab781313ad7 resolves the DATA preparation and recovery
return seams below; the scope review does not approve completed implementation.
The user now explicitly grants this full packet, including the limited unsafe
site, and continuous P3 work without repeated in-scope repair permission pauses.
All ownership, architecture, growth and independent-review duties remain. The
real full fixture reached its intended RED and now passes the connected path.
Same Heisenberg's whole51D SOURCE closure is Approve0/0/0 after411-hash and
recorded-gate checks. Connected-backing local result and progress.json own
exact evidence; aggregate P1, remaining matrix3/4 and five51B remain mandatory.

Goal: control original effect-container backing through real production
generation, Supervisor custody, Executor consumption and raw output transfer.
Full aggregate P1, pointee/map/Target/progress backing, other matrix3/4 duties and
all51B packages remain required; this packet does not close full P3 by itself.
No changes to K/E, public Vec item semantics, ACK ordering, authentication/CAS,
TCP interfaces, public TUN or external/commit/push authority.

#### Selected batch and error contract

Implementation seam correction under the user's continuous-P3 authorization:
same Reviewer confirmed that existing inner TCP mutators combine validation,
epoch/accounting checks and mutation. Preserve their original error order by
moving complete send/receive commit operations to private `tcp/send.rs` and
`tcp/receive.rs`, first behavior-preserving and characterized. Then introduce
prepared operations holding the same window's exclusive borrow; Session
prepares effects after all original checks, before infallible commit. Public
TCP signatures and the original preview order stay unchanged. No outer TCP,
getter, callback, re-export or alternate protocol implementation is added.
The bounded audit covers append/acknowledge/close, commit_receive/accept/
receive_close, and Session's existing final ownership/ID checks together;
it is not authorization to split these failures into later unbounded repairs.
This implementation correction is not runtime acceptance. Pure Move comes
before behavior; the Integrator lowers the exact inner TCP ratchet on shrink.
SOURCE01a0adc7-798f-7380-b92b-ca6f8981c62f additionally permits exactly two
`will_have_contiguous` bool queries on PreparedTcpReceiveData/Accept, scoped
`pub(in crate::resumable)`. These are exclusive prepared-result semantics,
not window getters: first-key/prepared-first or partial/exact-key lookup,
no allocation, mutation, full-map scan, borrowed view or re-export. Session
combines them with its original sink/pending/half-close conditions to avoid
allocating effects for gap/no-output paths. File envelope remains unchanged.

- Inner crate-private `SessionEffectBatch` owns the original Vec and one inline
  overflow effect for the first impossible full invariant. It is not Clone.
  The append site stores the original overflowing effect and immediately
  propagates the first error; no overwrite, further append or pool return.
  Same Reviewer SOURCE01a0ae05-6447-7413-8e45-a812993e82a4 accepts a monotonic
  mutation-start marker on this same batch: empty gap/expiry commits are not
  mistaken for precommit failure. Mark before actual Model mutation, not entry
  or mere allocation. Only empty completed transfer rearms it; installing a
  prepared special batch preserves the marker. Supervisor may restore its
  idle batch only after an explicit returned error with false marker, empty
  effects/facts and no committed journal frontier, never unknown/unwind/DATA.
- Ordinary Owner construction prepares capacity3 before source-factory mint.
  The original Supervisor owns that backing while idle and while its receipt
  journal is entered. Original rules write into that prepared batch through one
  inner borrowed reduction entry. Replace nested effect Vec/extend production,
  not the reducer rules. Empty backing is returned only after complete normal
  transfer and before another callback. Error/unwind batches never enter the
  normal pool. DATA keeps its original staging-before-Healthy/origin sequence;
  no newly fallible producer allocation after DATA is decomposed, and no new
  TCP getter or reconstruction of UplinkData from staging.
- Public `reduce` remains Vec-returning over the same rules. Existing validation
  and zero-effect exits precede lazy preparation; all preparation precedes the
  first mutation, not first append. Its impossible overflow must stay owned by
  a Model-held failed-batch position before returning a value-only SessionError;
  further mutation is rejected. This position is unused on the Owner borrowed
  journal path: it cannot become a second copy of that original batch.
- Recovery prepares its original exact batch inside the existing pre-CAS
  PreparedReplacement transaction. Do not grow the ordinary3 spare into R.
  Normal recovery conversion releases/reclaims only the now-empty original
  allocation, after an infallible transfer; retained failure keeps it owned.
- New SessionError storage reasons are value-only Layout/Reserve; ordinary
  impossible burst has a distinct invariant reason, never Reserve. Do not put
  non-Clone resources into the currently Clone SessionError. Existing causes
  retain their order and mappings; no universal catch-all replacement error.
- Leg's model-preflight error becomes a non-Clone carrying enum:
  `Existing(SessionError)` or `Storage { source: SessionError, received:
  LegBoundFrame }`. Only new recovery Layout/Reserve move the original seal/
  frame back into received. Existing maps to the old Supervisor Reducer cause;
  Storage maps to a new Supervisor carrying error. No new borrowed-preflight
  interface or authentication change. The existing cfg(test) direct forwarder
  uses the same owning return contract rather than discarding its new payload.
- Executor preparation rejects an untouched event/command/resume/recovery
  through the existing carrying category plus a specific storage reason.
  Entered-next-step preparation instead leaves the actual current/suffix/facts
  owned; it does not describe an already committed outer turn as uncommitted.
- Incoming holds the complete original batch plus original facts. If occupied,
  the original page retains its first batch; its typed conflict error owns the
  complete second batch, including overflow, preserving the existing error and
  observations Box identities. An observations-only failure carries an exactly
  empty batch. C continues to own the whole error, not an extracted summary.
- On registered restore failure, retain both the original Supervisor rejection
  (including received) and exact restore cause; the slot keeps its rollback
  custody. On catch-up reservation release failure, retain both that rejection
  and the returned exact reservation. Do not use `.is_err()` and replace these
  new carrying failures with payload-free reservation invariant errors.

#### Growth and unsafe boundary

Only `resumable/effect_storage.rs` may contain new unsafe, limited to checked
allocation and one owning Vec handoff. Match allocator/type alignment/Layout,
handle zero/ZST explicitly, test exact Drop and no leak/double ownership. No
handwritten element destructor, unchecked zero initialization, custom global
allocator, external allocator dependency or Vec layout-field assumption.

Work maximum is checked K+3 with first capacity3; raw output maximum is checked
E+1 with first capacity1. Reuse retained capacity. Growth chooses the first
clipped doubling level meeting need, never arbitrary capacity; check need<=M
and use c>M-c before doubling. With preceding terminal level p, maximum backing
overlap is p+M slots (M alone if there is one level). Check actual type Layouts
and simultaneous byte additions before allocation/move. Original producer,
raw output and any actual transfer scratch are additional, not counted twice
merely because the same allocation moves to a different owner.

Main checked593437 small-domain transitions of this proposed arithmetic plus
32/64-bit edges with BigInt; no counterexample. This is not Rust, allocator,
actual type Layout or total-phase proof. Runtime TDD must verify the chosen
levels and actual capacity. No bulk extend/collect that may grow during commit;
same-type Vec->VecDeque's guarantee does not cover cross-type conversion.

#### Exact requested write envelope

SOURCE01a0adb0-fc5d-7723-a66a-439374bf9a86 approves one additional private
ordinary producer child: move public `reduce` plus its24 complete helpers,
including `begin_half_close`, unchanged before converging the nested batches.
No parent forwarding, re-export or visibility expansion is needed. Preserve
both-mode Session44 and source equivalence for this unpromoted internal Move;
the final complete51D promoted change lowers the parent6442 ratchet to its
actual final count. Intermediate shrink is expected, not a green governance
claim. This placement approval does not accept the51D implementation.
Two further characterized pure Moves use existing deep children: complete
Supervisor `apply_event_observed_inner` to owner_observation (48/48 before and
after, body-equivalent; pub(super) retains original Supervisor scope), and
complete Executor `resume_attached_recovery_effects_observed` to observed_turn
(exact acceptance-queue tracer before/after, unchanged owning signature).
Subsequent backing changes are separate from these Moves; their final parent
ratchets are lowered together in the completed packet, not bypassed.

Paths below are repository-relative, not directory grants. Existing oversized
files remain at or below their current ratchet; this packet does not authorize
growth allowances or uncharacterized structural Move mixed with behavior.

| Production path | Allowed axis |
| --- | --- |
| src/resumable/mod.rs | One crate-private effect_storage route/type availability; no public API export. |
| src/resumable/effect_storage.rs (new, <=1500) | Exact allocation/growth primitive and original batch storage/overflow custody. |
| src/resumable/session.rs (<=6442) | Same-rule prepared ordinary/recovery production, public-wrapper failed-batch custody, value-only storage/invariant errors. |
| src/resumable/session/effects.rs (new, <=1500) | Complete ordinary dispatch and effect-producing operations; pure Move first, then one prepared batch and original preflight/commit rules. No public Interface expansion. |
| src/resumable/tcp.rs (<=2469) | Private implementation routes and behavior-preserving movement of the six complete mutating transactions; preserve public signatures and preview ordering. |
| src/resumable/tcp/send.rs (new, <=1500) | Original append/acknowledge/close transactions, then resumable-scoped exclusive prepared operations with exact preflight deltas and infallible commit. |
| src/resumable/tcp/receive.rs (new, <=1500) | Original commit_receive/accept/receive_close transactions, then the same exclusive prepare/commit separation; no receive map/backing redesign. |
| src/owned_upstream/leg.rs (<=2705) | Exact pre-CAS model-storage rejection/frame return and existing test-forwarder signature. |
| src/owned_upstream/supervisor.rs (<=5439) | Original producer backing/journal state, constructor/return route, storage error mapping. |
| src/owned_upstream/supervisor/driver_input.rs | Borrow original prepared batch without changing DATA entry/quarantine or Client disposition. |
| src/owned_upstream/supervisor/owner_observation.rs | Original receipt/batch/fact settlement and empty-backing handoff only after success. |
| src/owned_upstream/owner_target.rs (<=8515) | Original batch/storage/error anchors, private route, registered/catch-up failure custody and necessary source-route characterization adjustments. |
| src/owned_upstream/owner_target/effect_storage.rs (new, <=1500) | Deep Executor-local prepare/transfer/recycle operations over original queue/current/raw outputs; no second queue. |
| src/owned_upstream/owner_target/construction.rs | Checked Layout/initial storage before source-factory mint; exact original constructor rejection. |
| src/owned_upstream/owner_target/observed_turn.rs | Original batch entry, pre-effect preparation, conflict custody and E+1 raw transfer. |
| src/owned_upstream/owner_target/service.rs | Prepare before consuming/marking the real Driver receipt; preserve returned input/charge. |
| src/owned_upstream/owner_target/completion.rs | Prepare before the selected original completion's next side effect; no new scan/work owner. |
| src/owned_upstream/owner_target/read_source.rs | Prepare before actual Target read/source consumption; no post-read allocation rejection. |

Requested tests: new private `src/resumable/effect_storage/tests/mod.rs` index
(<=300), `.../tests/capacity.rs`, `.../tests/model.rs` and private
`.../tests/model/terminal.rs` leaves (each<=800).
SOURCE01a0adbe-f8ee-7241-915e-1520408e3220 approves moving the two Model
precommit tests/fixture intact into the latter before further Model tracers;
the original14 primitive/TCP tests return to the exact419-line capacity leaf.
SOURCE01a0add7-6109-71d0-87c1-3f592dedd9aa permits the intact graceful-finish
test Move into that terminal child before further natural terminal-failure
tests. Ancestor fixture access needs no new visibility or shared public helper.
Under continuous P3 authority, the same ordinary producer may declare the
private `src/resumable/session/effects/tests.rs` leaf (<=800). It challenges
the public wrapper's actual failed-batch custody through a deliberately
undersized original allocation; no public test getter or synthetic effect.
This is an impossible-capacity protection test, not a natural >3 burst claim.
One private route in existing
`src/owned_upstream/owner_target/tests/r7_initial_bootstrap_handoff.rs` and new
`.../r7_initial_bootstrap_handoff/effect_storage.rs` (<=800); its new private
`.../effect_storage/faults.rs` and `.../effect_storage/recovery_faults.rs`
hold complete refusal/incoming/abort and catch-up fault witnesses, each<=800.
The first three fault tests moved unchanged with four-test characterization
before adding defensive guard REDs; ancestor fixtures need no new visibility.
Their checked bootstrap uses production prepare_owner(plan), not the old
unproven fixture. E1 is supplied at construction. Exact existing
nested-error construction/destructure in `src/owned_upstream/
owner_session_controller/tests/terminal_grace/returned_error.rs`, preserving
all Box/fact identity checks. Existing `tests/knife16_owned_leg_provenance.rs`
(<=2624) may adapt only the changed owning model-preflight error signature and
its exact source route, never weaken a guard or append unrelated tests.
The exact existing retry-write-wake test in owner_target/tests/r7_observed_pages.rs
may replace its old cap0 assumption with the actual original pending backing
pointer/nonzero capacity moved into the page. All wake, payload, inventory and
reentry assertions remain; no claim that the whole call is allocation-free.
Same Reviewer approved this isolated characterization adjustment0/0/0.
Any additionally forced caller/test write is a scope stop, not a directory grant.
No C/standby/auth or owned_upstream TCP production write is granted by this proposal. Governance
inventory is Integrator-only after actual shrinking; no proactive ratchet edit.

#### Executable acceptance, not helper-only completion

First executable fixture is `checked_owner_effect_storage_survives_registered_recovery_and_reuse`
in the granted initial-bootstrap private child. Use checked replay geometry4,
rate4096bit/s,500ms horizon,32-byte segments, Z4 and sink64; E1 and derived K,
Source64 and real MemoryTarget. Existing MemoryLegTransport/TwoLegWire drives
both initial request/response and B registration/exact-next ATTACH; actual peer
receive and typed validation retire ordered waits, never manual mark_delivered.
Client uses production bootstrap_initial; Owner uses prepare_owner(plan) then
from_initial_bootstrap and retains the entire initial handoff. Three actual OPENs
give R4 (LegActivated plus three OpenResults), four real one-step pages and E+1.
Observe ordinary original batch at the existing reduce_owner_command_observed ->
execute_effects_observed boundary on each side of recovery, preserving both facts
and ownership; no new observation getter. Record cold/work/source capacities and
source pointer before asserting them AFTER complete protocol/recovery/reuse.
Predicted first runtime RED is cold work backing0 versus required3; intermediate
protocol/authority failures are fixture feedback, not the intended behavior RED.
This proves the component storage path, not physical QUIC/root lifecycle or WAN.

First real tracer must construct with production checked capacity/bootstrap,
retain the original handoff, drive actual protocol/Target work, cross real
recovery/continuation growth, and reuse the same ordinary backing. It checks
cold non-dense allocation, actual capacity and retained empty backing, exact
effect/output order and identity, raw E+1 and unchanged Target execution counts.
Use production registered/catch-up acceptance, not accept_owner_attach_for_test.
The exact test must fail on missing backing discipline, not a substituted
producer/consumer or a manufactured successful port. A mere cold-capacity
assertion is not the completion boundary. Freeze concrete fixture/first RED
before its write; if it cannot reach that complete path, revise this packet
explicitly instead of reducing acceptance to the helper test.

Required rejection/fault rows: real unrepresentable constructor Layout before
mint with original inputs returned; prepared allocation refusal before the
next side effect; normal drain/reuse; first batch overflow; occupied incoming/
conflicting facts retaining both original batches; DATA validation/poison
precedence; public no-op/error precedence; recovery pre-CAS exact frame return;
registered restore and catch-up release defensive composed-error custody.
Allocator refusal injection is not evidence of real OOM recoverability.
Existing both-mode OwnerTarget/Supervisor/Controller/Client/CurrentOnly and
recovery regressions, Knife/repository provenance, strict standalone/Cargo
growth, scoped formatting/diff/check and same independent review remain.
No full aggregate P1/P3 closure or external acceptance from this packet alone.

### E. Matrix3 original nested-error cold audit, finite first family

Continuous P3 authority applies; same Heisenberg SOURCE0/0/0 approved this
finite implementation envelope, not the complete inventory P1. Actual recovery
currently produces only LegActivated/Transmit, no Target callback/await in its
held-cursor window; no new panic hook may masquerade as natural interruption.

The next executable invariant is original error payload audit following its
unique owner: unknown C aborted_turn versus known returned error in the original
output failure ledger. C ordinary facts, nested facts and Executor residuals
are distinct domains; do not add their snapshot summaries twice or deduplicate
different owned facts merely by terminal identity.

Production paths: owner_target.rs diagnostic route; new owner_target/error_inventory.rs;
owner_session_controller.rs nominal anchors; owner_session_controller/cold_inventory.rs;
quinn_leg_composition/owner.rs nominal inventory; owner/inventory.rs; and original
owner/continuation.rs failure-ledger/output_inventory composition. The new deep
diagnostic module is visible only within owned_upstream; value types match the
visibility required by existing result interfaces. No business getter, raw-error
writer, re-export, callback, allocation during audit or resource transfer.

Supported first family: observation-retention/conflict wrappers, their single
inner-error chain and explicitly exhaustive pure-value Target/Supervisor errors.
Borrow iteratively with checked counts, no recursive stack or allocated worklist.
Other carrying domains return typed UncoveredDomain, never partial-success zero.
Even an empty effect batch with capacity or mutation_started is uncovered until
audited. C and Root preserve typed failures; Invariant/uncovered/overflow cannot
pass zero. This partial interface does not discharge full effects/bytes/cursor/
received/command/reservation duties elsewhere in51A.

First falsifier extends existing returned_error.rs real RESET/exhaustion plus
explicit defensive wrapper: known returned error has nested fact1 and C0;
existing-hook unknown has C nested fact1 and ordinary facts0. Original Box,
terminal/window/tail survive two rejected reentries. Same finite packet connects
the actual output failure ledger and tests supported audit, Uncovered and checked
overflow. No C private fixture export to a sibling merely for Root coverage.

Pure Move prerequisite is accepted0/0/0: C106+154 lines and owner202 lines moved
as complete implementations, storage/nominal anchors stay parent. Original143-line
provenance test moved to its explicit private controller_inventory.rs child,
then exact source routing followed the real child with AST route/absence checks.
Current exact C3009/child268, owner1861/child207, Knife2454/child160; three
Integrator ratchets lowered. Existing returned_error.rs and continuation
tests/transactions.rs own behavior tests; Knife parent/new provenance child own
exhaustive field/routing guards. New files obey normal limits; later anchors
must fit the lowered ratchets. No governance growth allowance is authorized.
Move evidence: /tmp/mini-vpn-p3-nested-inventory.q0LWgl/move.json; both-mode
C61/owner9/continuation16/Knife190/repo20/growth7/check and scoped gates green.
Finite-family implementation is now accepted, same Heisenberg SOURCE0/0/0.
Original nested RESET fact/cause follows known returned error or unknown C;
TargetAndProgress counts target once. Root tests use actual checked scalar
rejection and deliberate WrongActorClock, not natural nested-conflict claims.
Uncovered effects/storage/input/other domains stay typed audit failures; checked
overflow and explicit Invariant also fail zero. Current C3007/cold332,
owner1860/inventory220, error child280, continuation1355, test leaves464/618,
provenance child205; two exact Integrator ratchets lowered. Both modes:
C64/owner9/cont17/CurrentOnly13/server25/Knife190/repo20/growth7/check green;
two exact tracers each10 repeats, fresh strict standalone7 and scoped gates green.
behavior.json/behavior-gates.json own415 hashes and evidence. Full effects/bytes/
cursor/received/command/reservation inventory and all51A/51B/P3 duties persist.

Recovery cursor/resume error subfamily is now locally accepted, same Heisenberg
SOURCE0/0/0 after its scope review. Only error_inventory.rs, checked-protocol
effect_storage.rs test leaf and returned_error.rs metric counterexamples change.
No production parent, C interface, getter/extractor, queue or business behavior.
Add a nested RecoveryPayloadInventory with original cursors/output values,
pending/ready capacities and their checked element-array Layout bytes, frames/
checked wire lengths, resume tokens, attached authorities and acceptance receipts.
These are owning-token counts, not actor/FIFO/endpoint/session counts or valid-
correlation proof. Array bytes exclude pointees/shared metadata/allocator overhead
and are not aggregate peak. Empty/checked_add/zero compose every subfield.
Audit pending/ready exhaustively and borrow the original error chain iteratively;
unsupported nested carrying domains keep the whole result typed Uncovered.
Single resume errors count no cursor; an actual empty cursor still counts1.

First falsifier extends the existing actual three-OPEN/E1 fixture: B FIFO8->3;
acceptance plus two OpenResults fills it; final OpenResult3 must return actual
AttachedRecoveryQueueBlocked. Existing error audit then REDs with Uncovered.
After minimum GREEN, consume the same error/cursor, actually receive acceptance
to free one slot, retry and preserve all three original peer-delivery assertions.
Hold that received BoundInbound for the original acceptance validation; no fake
queued frame, synthetic pressure flag or duplicate fixture. Leaf stays below800.
Nested wrapper, ready/pending/malformed retained custody and arithmetic/zero
counterexamples plus same related both-mode gates/review follow. This remains
finite error-payload evidence, not checked Bound C/Root or full matrix3 closure.
Same Reviewer also approves the private test-only placement extension:
effect_storage.rs declares bare private mod error_inventory; new descendant
effect_storage/error_inventory.rs owns explicit defensive audit holders and
counterexamples, not a new natural-failure or correlation claim. Ancestor helpers
stay private; parent/child both below800. Production scope does not expand.
Implementation: expected real FIFO3 Uncovered(Recovery) runtime RED -> GREEN;
original final OpenResult3/drain retained, actual acceptance receive then retry,
all original three peer-result/storage-reuse assertions preserved. Counterexamples
separately mark deliberate malformed/token holders, not valid recovery authority.
Single resume has cursor0; actual empty cursor1; all ten nested metrics participate
in empty/checked_add/zero. Unsupported queue ProtocolError remains typed Uncovered.
Current error module510, protocol leaf771/diagnostic child258, C arithmetic503.
Three old paths plus one new test leaf;416 hashes/412 unchanged, no inventory edit.
Both modes Target171/C65/owner9/cont17/CurrentOnly13/server25/Knife190/repo20/
growth7/check plus strict standalone7 and20 real-pressure repeats are green.
cursor.json/cursor-gates.json in the same artifact directory own exact evidence.
Next consolidate all remaining error-carrying domains and real borrow seams;
do not treat this family as C normal-recovery, natural-unknown or full-P3 closure.

### F. Remaining carrying-error audit — locally accepted implementation packet

Standing complete-P3 authority includes in-scope repair; this is one remaining
packet, not a sequence of per-scalar permission requests. Same Heisenberg
accepted the implementation direction with one scope P1: codec checked length
must be mandatory. The correction below closes that scope issue without another
architecture review; it is not approval of unwritten code. First real fixture
baseline passes in both modes; strict standalone growth7 passes.
This section consolidates51E's remaining duties without completing matrix3/4.

Entry is the original OwnerTargetError/ControllerError cold audit. The original
error or C/Root ledger remains the unique owner. Traverse the single nested
OwnerTarget error chain iteratively; Supervisor causes terminate in their own
input/value payload, not a second OwnerTarget branch. No recursion, allocation,
clone, mutation, callback, token extraction or business settlement in the audit.
Match every existing carrying variant explicitly. Unsupported representation,
invalid frame, checked count/Layout failure cannot become a partial-success zero.

Select transport-independent Session payload counts at a lower nominal owner:
one crate-private resumable/payload_inventory.rs traverses Frame, SessionEvent,
SessionEffect and the original SessionEffectBatch using existing borrowed ports.
It composes count, actual vector/string capacity, element-array Layout, original
overflow/mutation, DATA views and checked wire lengths. Segment backing charges
are per-view retained charges, never a deduplicated physical allocation sum.
No owned_upstream/Tokio/Target-specific nominal type belongs in this lower module.
Leg's private diagnostic child borrows its sealed Frame internally and returns
only this value summary. TCP's private diagnostic child retains its own value
type and counts actual staging/stored bytes and each actual byte/segment permit.
Only OwnerTarget's existing deep diagnostic module composes those local domains.
No raw Frame/ownership getter, re-export or reverse dependency on OwnerTarget.

Original effects, commands, received input, status publications, reservations,
pending loss and terminal recovery tails are all mandatory in this packet.
Count a reservation only at its actual holder: registered restore error does
not own the reservation still retained in its standby slot; catch-up release
error does. Receipt/arm/attached/terminal token cardinality is not an additional
FIFO, endpoint, ACK or session resource, nor proof of valid correlation.
Controller rejected actor input includes the original event and arrival witness;
its returned recovery error includes the original unexecuted recovery. Ordinary
C facts, error-held facts and Executor residuals remain separate domains.

Exact production paths: resumable/mod.rs (route), new
resumable/payload_inventory.rs; owned_upstream/leg.rs (net-zero route), new
leg/cold_inventory.rs; owned_upstream/tcp.rs (net-zero route), new
tcp/cold_inventory.rs; existing owner_target/error_inventory.rs and one private
error_inventory/payloads.rs for the complete owning-payload traversal if needed;
owner_session_controller/cold_inventory.rs. Leg-control wire length is mandatory:
protocol.rs gets only a net-zero private route to a new
protocol/cold_inventory.rs: allocation-free inherent checked length using its
existing validation/constants, not duplicated encoding logic. No negotiation
capability or resource is minted by that value-only method.

Exact test paths: existing initial-bootstrap effect_storage/faults.rs,
recovery_faults.rs and error_inventory.rs, one private descendant
error_inventory/remaining.rs; C terminal_grace/returned_error.rs; continuation
tests/transactions.rs; new resumable/payload_inventory/tests/mod.rs. Existing
old assertions remain except explicit partial-Uncovered expectations advanced
to the newly supported complete value audit. Every new leaf<=800, production
file<=1500; oversized route parents stay net-zero or shrink with Integrator-only
ratchet handling. No blanket directory grant, production unsafe or behavior edit.

First falsifier: extend the existing checked real DATA pre-entry reserve refusal
test entry_output_reserve_returns_original_data_before_reducer_or_target_commit.
Borrow the actual returned error before destructuring. Predicted runtime RED is
Uncovered(Input), not a fabricated failure or missing helper-only test. Preserve
the original payload pointer/bytes, unchanged Target/reducer, exact retry and ACK
assertions. Then cover original effects+overflow/backing, actual Driver permits,
received ATTACH+registered/catch-up refusal, status/terminal tails and C/Root
returned inputs. Deliberately malformed holders remain labeled defensive, not
natural production failures. Each summary field participates in checked sum/zero.

Exit: complete existing error-family traversal plus both-mode related/provenance/
growth/check/scoped gates, original-holder identity and same independent SOURCE
review. No normal-C-recovery, natural-unknown, aggregate peak, root-zero or P3
completion is inferred. Normal C recovery must subsequently reuse the same
borrow-only payload traversal against its real owning typestate, with its own
checked-Bound fixture; legacy adapter success cannot replace that duty.

Accepted SOURCE: same Heisenberg P0/P1/P2=0/0/0 after independently matching
423 hashes,18 exact paths (11 old+7new),405 unchanged. Eight real focused
RED/GREEN steps cover DATA, effects, Driver, received ATTACH, status/reservation,
recovery, returned C input and terminal cursor. Additional tests lock original
normalized replay/sink views, all five diagnostic terminal tails, classified
control rejection, C returned-recovery retry, invalid frame and every count axis.
Diagnostic malformed tails and the legacy C returned-error fixture are not
normal checked-Bound recovery or valid physical terminalization acceptance.
Two test-only feedback repairs fixed sum inference and the pre-normalization
pointer baseline. Standalone Knife required explicit Leg child path and
equivalent ancestor-relative visibility; no provenance test or scope widening.
Main ran32 final gates,80 exact repeats; all green in default/all-feature modes.
Strict standalone growth7, scoped18fmt/diff/120 text-whitespace checks pass.
Parent debt remains leg2699/tcp2828/protocol1992; new leaves<=800/index221.
Reviewer ran no Cargo. remaining{,-gates,-repeats}.json in the established
artifact directory own exact evidence. No further wording-only review required.
Normal C owning recovery/status/high-water audit is the next mandatory matrix3
dependency, followed by applicable interruption/reachability and matrix4/five51B.
No aggregate-peak, physical-zero, complete matrix3 or full-P3 claim.

### G. Normal Controller recovery inventory — locally accepted packet

Mandatory matrix3 dependency, not a new sidecar or replacement lifecycle.
The real checked test `checked_bound_recovery_retries_the_same_publication_after_real_fifo_pressure`
already enters C through production registered admission and real FIFO1/E1.
Current C cold inventory sees ordinary pages and error-held payloads but not
the original normal recovery/status/high-water/quarantine holders. Snapshot
markers are not their nested payload inventory. This is a diagnostic coverage
gap, not evidence that the existing physical-root zero predicate lost ownership.

Entry stays C::cold_inventory, borrowed at the control/cold boundary. C remains
the sole owner of recovery, attach_status, legless_high_water, terminal_quarantine
and aborted_recovery. Visit each actual field once. Keep ordinary error payload
and normal recovery payload separate; do not add snapshot-derived counts into
either owning walk. Executor residuals and actor/FIFO/timer/session remain
separate physical domains. Returned-error traversal remains unchanged in meaning.

Reuse51F's actual payload walkers through inherent owner-scoped value methods
on existing Target-owned types, defined beside their private implementation.
Prefer converting existing private free walkers into those methods with their
old callers migrated, not adding pass-through wrappers or exposing private
fields. No re-export, C nominal dependency in Target, callback, getter, clone,
new owner or behavior change. C matches six recovery phases, two status phases,
the high-water holder, exact terminal quarantine/cause, aborted pending/results/
facts. Array capacities count their actual retained positions; a result array
without a cursor must not invent one. Empty aborted guards remain nonzero via
their existing guard count; checked traversal failure makes cold zero false.

Value interface: one new `retained_recovery_payload: ErrorColdInventory` field
on existing C cold result, one owner-scoped accessor, and its zero check. Normal
traversal failure maps to a typed `RecoveryPayload(ErrorAuditFailure)` outer C
cold error; the existing physical caller then sets audit_failed and nonzero.
Do not hide that failure inside a new inner Result invisible to that caller.
Existing error_payload remains error-only. No new root builder/aggregate detail.
Audit cost O(original held effects/outputs/error nodes/segments), no allocation
or packet hot-path call. No new numeric capacity bound is claimed; matrix4 owns
actual aggregate peak/call charges.

Exact executed files: owner_session_controller.rs (field/error/docs net-zero at
3007), owner_session_controller/cold_inventory.rs; owner_target/error_inventory.rs
and its payloads.rs; tests/knife16_owned_leg_provenance/controller_inventory.rs
(add actual cold field and bind Target cold-method count to its exact impl owner,
preserving the complete structural guard rather than relaxing old coverage).
Tests: existing owner_target/tests/r7_initial_bootstrap_handoff/effect_storage.rs
and new private effect_storage/controller_inventory.rs; existing C
tests/terminal_grace/returned_error.rs only for owner-local diagnostic cases if
needed. No other Rust/governance edit; all ordinary limits apply.

First do a pure characterized Move of the entire checked FIFO-pressure test
from effect_storage.rs771 into that child (about260 lines plus docs/import).
Parent adds only private Module route and loses the original body. Baseline,
same moved test, full effect-storage component/provenance/growth/scoped checks
must pass before behavior changes. No visibility/helper extraction required.
Then add the first falsifier at its real C-held Publish/HandoffReady boundary:
cold inventory must not be zero while it owns the actual cursor/handoff.
Prediction: runtime false-zero RED, not fixture/privacy/compile failure. If the
old predicate is already nonzero, require exact new payload-field observations
as the distinct falsifier; do not force a production failure. Continue with
numeric original frame/array/attached/receipt counts across three pressure
returns, same retry and actual peer delivery; original assertions stay intact.

Exit is both-mode related/provenance/growth/check/scoped gates, original-holder
tests and same independent SOURCE review. Unknown/fatal/cancellation claims
must use actual existing entry points or explicitly labeled diagnostic states;
no fabricated natural unknown or new panic seam. Any unexpected red is diagnosed
before a scoped repair. Scope/Authority or growth escape requires redesign, not
a bypass. Same Heisenberg final SOURCE0/0/0 accepts this finite packet;
local Module/Clean/System/DDIA scores9/10. Matrix3/4/five51B/P3 stay open.

Execution: original255-line checked test moved unchanged (hash701ea21d...a74d),
parent771->516/new259; Move component18/provenance195+20/growth7/scoped checks
green before behavior. Actual C-held Publish then produced exact false-zero
runtime RED. Borrowed normal traversal GREEN preserves all three pressure
returns, forbidden reexecute/republication and real exactly-once peer delivery.
New numeric checks cover Initial/Execute/Publish/HandoffReady, retained array
Layout and separate error-only ownership. Defensive C aborted pending/results,
empty guard, high-water/quarantine causes and original frame/backing add finite
coverage, not natural interruption. Protocol-private invalid construction was
not widened: lower malformed tests and AST typed-outer propagation remain.
Final8-path snapshot424 hashes/416 unchanged; C3007/cold396, Target694+467,
provenance267, tests516/501/693.24 final gates and60 exact repeats pass both
modes; SOURCE reviewer independently checked hashes, ran no Cargo. Existing
local-results and normal-c*.json own exact evidence. No governance/external/git
mutation. Next applicable reachability/interruption, matrix4 and five51B.

### H. Current first-path matrix3 applicability — locally accepted

Same Heisenberg SOURCE0/0/0. The one missing checked-Bound typed postcommit
failure witness is now real: after original OPEN, the existing resume-ID
exhaustion seam precedes checked C construction; recovery returns exact Target
failure before its suffix moves. Executor retains original cursor, one output,
one remaining effect and attached authority; C becomes Failed, caller owns the
error, C does not duplicate it. Two execute/publish reentries preserve original
full C/FIFO inventory. Original success path inverse hash remains exact.
Only existing test child501->575; no production/new hook.24 gates/40 repeats
green,424 source hashes/423 unchanged. fatal{,-repeats}.json in the existing
artifact directory and local-results own commands and independent review.

Applicable mapping: actual FIFO pressure is covered by G's original cursor,
retry and peer delivery; error and normal payload walkers by F/G; borrowed
shutdown observer cancellation/reentry by existing CurrentOnly runtime tests;
typed checked fatal return by H. Current replacement producer emits only
LegActivated/Transmit, synchronous current-first processing, with no identified
Target callback/await or natural payload Drop callback at this recovery seam.
That unknown row is SOURCE not-applicable-now, NOT unknown runtime PASS or
universal unreachability. No fake panic is introduced to populate the matrix.
Legacy B-only post-bind and prototype scheduler tests remain separate evidence.
Allocator abort remains its explicit model; recoverable Layout/Reserve belongs
to matrix4. New producers/callbacks in51B must reopen affected applicability;
parent destruction, full shutdown and whole-root zero remain P4–7. Aggregate
matrix4/P1, combined consumer review and all five51B remain required for P3.

### I. Original TCP sender descriptor backing — locally accepted finite packet

Scope and final SOURCE reviewed by same Heisenberg0/0/0; only sender accepted.
Real route: Session local_data -> original TcpSendWindow::prepare_append ->
effect preparation -> original prepared commit. Original send window remains
sole Authority. Existing commit push_back can implicitly grow beyond logical
max_segments. First falsifiable RED: real one-segment append must retain actual
capacity1. No synthetic peak or wrapper-only witness substitutes for it.

Prepare private Reuse/Replace backing inside the existing append guard, without
changing original deque. Bound clipped growth by actual capacity/len+1 and
max_segments; check old+new actual Layout before allocation. Preserve original
validation and backing-accounting error precedence. Drop or later effect reserve
refusal keeps original pointers, both slices, payloads, capacity and counters.
Commit moves originals into prepared capacity then same-type Vec->VecDeque,
publishes accounting and only then drops original Bytes; no descriptor allocation
or user element Drop during transfer. ACK does not erase retained capacity.

Production scope: effect_storage.rs (one shared unsafe primitive, effect-domain
TLS preserved including Layout/zero/ZST priority); tcp/send.rs (preparation,
guard, commit and private test route); tcp.rs (value-only descriptor Layout/
Reserve errors plus compact docs, net-zero1995); owner_target/error_inventory/
payloads.rs (exhaustive scalar causes, no wildcard). No new unsafe site.
Tests: new tcp/send/tests/mod.rs private index and descriptors.rs; existing
effect_storage/tests/capacity.rs and model.rs. The new layout follows project
test rules rather than the review's suggested tests.rs shorthand. No getter,
Session/Executor production change or effect fault-domain conflation.

Acceptance: real1->2->4->5 growth/wrapped deque, old+new Layout, ACK retained
backing/reuse, descriptor Reserve and preparation-drop exact rollback, original
effect Reserve after prepared growth and successful retry, real Model/global
state and unchanged Executor effect-reuse test. Scoped/default/all-feature TCP,
storage/Session/Target/C/Root/provenance/growth/check gates and independent SOURCE
review. Unexpected failures require evidence/failure-tree before in-scope repair;
no repeated user permission needed. New architectural scope is not implicit.
Receive/map/Target backing, aggregate peak/call charges and all five51B remain
mandatory; this finite sender transaction cannot promote matrix4/P1/P3.

Executed evidence: first actual append left4/right1, then minimumGREEN; shared
primitive-only extraction first reran original storage25/25. Private sender5,
storage26/TCP32/Session47/Supervisor48/Target179/C67/owner9/cont17/CurrentOnly13/
server25/Knife201/repository20/growth7/check pass both modes, standalone7 and
scoped8fmt/diff pass:34 final gates. Real Model second-growth/effect refusal and
unchanged Executor effect-reuse each10 per mode:40/40. SOURCE426 hashes,
6 old changed/2 new/418 unchanged; tcp1995/send397/storage270/audit469,
tests186/index1/capacity440/model665. No debt edit. Existing warnings unchanged;
no globalfmt/full-library/RSS/OOM/aggregate peak acceptance. Artifacts live in
/tmp/mini-vpn-p3-descriptors.FVL93c/; existing local-results own durable summary.

### J. Complete receive storage and Offer preparation — locally accepted

Whole-map SOURCE review by same Heisenberg retains original aggregate P1 at
0/1/0; no new safety/OOM incident. Stable BTreeMap's public API cannot turn
element bounds into a checkable requested-node Layout or prepared-node reserve.
Header size, guessed private constants and try_insert do not supply that proof.
Sorted Vec changes important ordered insertion/partial accept costs to O(R);
moving arena expansion has an O(R) relocation spike. Neither is an implicit
equivalent replacement. Preserve existing ordered operation complexity.

Main's selected next direction is nonmoving, original-owner exclusive ordered
nodes and recycled slots; not a generic BTreeMap API clone or global allocator.
The candidate can reuse the sole shared unsafe primitive: exact Vec<Node>(1),
initialize one vacant node, then safe Box conversion at len==capacity==1. Local
rustc1.95 public docs guarantee Vec->Box<[Node]> does not reallocate/move at
equal len/cap; checked Box<[Node]>->Box<[Node;1]> is in-place and returns its
original Box on length failure. This proves conversion only, not the full tree.
No new unsafe site, arbitrary Ord callback in commit, relocation, heap traversal
stack or audit-only allocation is permitted. Node slack, recycled/ready nodes,
actual allocation-call cost and fixed iterator state all require accounting.
Same Heisenberg selects safe owning-node AVL and the complete transaction as
the execution design exit (SOURCE0/1/0 retains the original unimplemented P1).
With L live, F free, A=L+F and U newly inserted ranges, missing=max(0,U-F),
prepared peak=max(A,L+U). Validate real Node Layout and original range bound.
Prepare only new missing nodes, leaving old free chain and tree untouched;
partial accept reuses its existing node. Rotations move owning links only.
Free/prepared chains must drop iteratively, not recurse through up to R nodes.
Fixed ancestor stack requires the actual AVL height invariant; no truncation.
New resident allocation is business storage, not an audit allocation; no claim
of equal allocator cost or throughput. Warm reuse must allocate no node.

Frozen full production packet: tcp.rs, tcp/receive.rs, new private
tcp/receive_storage.rs, session/effects.rs, session.rs, original exhaustive
owner_target/error_inventory/payloads.rs; existing allocator unchanged.
Storage private tests and existing capacity/model trees own evidence; new
model/receive.rs can carry the real connected fixture. Public peek retains Vec
and allocator-abort semantics; Session instead prepares exact owning Offer
backing before mutation. Borrowed length/boolean queries do not allocate.
Internal TCP committed operations must retain both original input Bytes owners
until Session global accounting and actual Offer publication, before user Drop.
Preparing only the tree does not close this callback/fallibility transaction.

First preparatory phase is a pure Move: complete preview_receive docs/body
from tcp.rs to its existing receive.rs impl, same Interface/owner. Original
bridge characterization and block hash are fixed; TCP/storage/Session and
provenance/growth/scoped gates follow, then Integrator exact shrink ratchet.
No node/Offer behavior is mixed into the Move. After its reviewed boundary,
freeze the full Model gap/bridge/Offer/partial/reuse fixture and exact allocation
failure phases before behavior TDD. No further user permission pause in scope.

Preparatory Move accepted: same Heisenberg SOURCE0/0/0,426 hashes/3changed/
423unchanged.195-line block hash0e58936587cc6a8c3b00137b072411b2751acde6eee980333f6d59ad450ed33d;
parent1800/child603; parent/child/inventory inverses exactly match51I. Bridge
baseline/moved1/1,20 final gates green, strict standalone7. Integrator alone
lowers tcp debt1995->1800. No new Interface/route/behavior. receiver-move*.json
in the51I artifact directory owns evidence. Next full fixture/faultpoint freeze
and complete behavior TDD, not another prerequisite-only design sidecar.

Next complete vertical transaction, not two isolated scratch Vec repairs:
original receiver ranges; overlap/uncovered preview; normalized ranges and
node preparation; receive commit; prefix accept/partial-key relocation; abandon;
contiguous output descriptors. Session receive commits before offer_contiguous
today, so a new recoverable Offer allocation cannot be introduced after that
commit. Sink accept and OPEN activation have the same obligation. Existing
peek_contiguous calls in sink-offer exhaustion and activation prechecks need
only borrowed length/contiguity, not new arrays. Preserve original public/error
contracts and move all newly fallible preparation before the corresponding
mutation; no callback/optional-control proliferation to disguise the work.

Current implementation is locally accepted by same Heisenberg SOURCE0/0/0.
It exercises genuine gaps2:cd/6:gh and bridge0:abcdefgh, node/Offer/reservation
refusal/retry, partial accept and warm reuse; both OPEN backlog results remain
unrecorded on Offer Reserve. Original custom Bytes unwind follows TCP/global/
original externally-owned batch publication. Defensive Overflow retains ACK
prefix and complete Offer segments; global bytes/range errors precede Reserve.
Node addresses/balance/live/free partitions, intermediate allocation refusal,
8192-node small-stack prepared Drop and linear clear are covered. Clear's
1410-vs256 RED is corrected to one visit per node, not repeated AVL removal.
No generic storage Interface/new unsafe or aggregate memory/throughput claim.
Current source430 hashes/12paths/418 unchanged;31 gates+40 module repeats/220
test executions green. Both modes TCP36/storage33/Session47/Supervisor48/
Target179/C67/owner9/continuation17/CurrentOnly13/server25/Knife212/repo20/
growth7/check; strict7/scopedfmt/diff/untracked checks pass. Integrator only
ratchets tcp1800->1770 and Session5613->5585. The two former single-use global
mutating helpers are replaced at their existing owning call sites by checked
complete usage values prepared before commit and assigned once at commit.
`/tmp/mini-vpn-p3-receive.T55xyv/closure-{source,gates,repeats}.json` owns final
evidence. Review found one public-capacity-guarantee P1: Vec bulk extend may
reallocate despite capacity. Same three lines now use single-item push;819 lines
unchanged and inverse hash exact. Reviewer0/0/0 Approve, no Reviewer-run tests;
main reran all31 gates/40 repeats after repair. review.json retains both verdicts.

Remaining owner map duties are unchanged: Session OPEN before adapter callback,
finish before live->tombstone move; Supervisor staging/ACK receipts before
permit release (candidate collection currently includes release); MemoryTarget
live->joined preparation and retained buffers; OwnerReverseProgress original
ordered key-resume scan, revision and 2*(F+Z). Its bounded scan must not become
whole-table filtering. Other maps migrate only at their actual complete owning
transaction, not by a blanket rewrite. Aggregate peak/combined consumer/all
five51B and P3 remain open. Current430-path implementation is the frozen locally
accepted receiver transaction, not aggregate/P3 acceptance. Next symbol-first
boundary is the original Session flows/tombstones OPEN/terminal ownership.

### K. Original Session flow/terminal storage transaction

Selected finite execution direction: same Heisenberg SOURCE0/0/0, local
Architecture/Clean/System/DDIA9/9/9/9, not implementation/aggregate acceptance.
Keep two native SessionFlowId ordered roots (live and terminal) under one
original Session owner with a recycled vacant-node pool. A same-layout owning
node moves live->terminal; never swap another key/payload in its place. Live
lookup remains O(log F), terminal O(log Z), counts O(1), scans O(F)/O(Z).
Recovery still emits all ordered terminal controls before all ordered live
flows. No generic comparator/map API, new unsafe site, relocation, callback,
public getter or heap traversal stack. The shared exact allocation primitive
and checked in-place Box conversion are the already accepted mechanism.

Design exit/next executable path: original LocalOpen preflight -> existing
Adapter -> original Model commit, plus Peer OPEN and terminal/expiry through
the same owning storage. Pure Move precedes behavior: retain PreparedLocalOpen
nominal type and recovery/validation in parent; move its whole impl and original
local/peer OPEN, terminal precheck/finish, both expiries and their existing
effect publishers into private session/flow_lifecycle.rs. Only equivalent
Session-scope visibility restores sibling callers, not re-export/pass-through.
Freeze the genuine retire3-before1 recovery test: terminal1,terminal3,live2.
It has passed before/after Move; initial test mistakenly expected ACK for an
empty receive window, corrected against the existing conditional, not production.
Move source432 hashes/6 changed/426 unchanged; six original bodies exactly
match after removing equivalent visibility. Session5585->5380, effects1242->1005,
child451, model index667/test51, Integrator ratchet5585->5380. Twelve Move gates
pass; /tmp/mini-vpn-p3-session.xmyomq/move-{plan,source,gates}.json owns evidence.
Move independent closure is SOURCE0/0/0 Approve, including both full parent
inverse hashes. All12 Main gates are green; Reviewer ran no Cargo. Node and
transaction behavior follow under the same finite scope, not accepted by Move.

Behavior scope: original session.rs, session/effects.rs, flow_lifecycle.rs,
original exhaustive owner_target/error_inventory/payloads.rs; real Model
lifecycle test and private flow_lifecycle/tests index/leaf. No other map or
allocator change. Governance edits are Integrator-only shrink ratchets. New
production/test/index limits1500/800/300 remain, no new grandfathered entry.

Mandatory acceptance, not independently deferrable container-only subclaims:

- Local OPEN preserves original capability/role/target/F/ID/effect refusal
  order. Newly missing physical node prepares before Adapter. Rejection/drop
  returns original target/input and leaves old tree/free pool/IDs unchanged.
  Peer duplicate/tombstone/high-water branches do not prepare a node.
- With L live,T terminal,V vacant,A=L+T+V, one OPEN needs a node iff V==0;
  peak=max(A,L+T+1). Independently preserve F/Z and check all actual counts,
  F+Z and real Node Layout through typed precommit errors, not unwrap in
  infallible SessionModel::new. No dense maximum allocation. Terminal/vacant
  nodes retain the larger live-sized header; charge it honestly. Original
  TCP/target/effect backing and concurrent retired FlowState remain separate.
- Complete precheck precedes terminal commit. Transfer the same FlowId's
  original owning node, retain its original FlowState through counters,
  tombstone and actual FlowFinished publication (including inline overflow),
  then dispose. Test original-node identity on two-child tree deletion.
- Exact TerminalGrace expiry returns that node to vacant pool. Resume expiry
  detaches complete roots/pool, publishes counters/Expired/SessionExpired to
  original batch, then disposes. Linear traversal/bounded tree depth and
  iterative free-chain Drop are required. Test-only destructor injection
  is not a claim of a natural callback in normalized production payloads.
- Real tests cover refusal/retry, Adapter rejection/unwind, both OPEN roles,
  F/Z/ID precedence, terminal migration/reuse, expiry/disposal and ordered
  recovery. Both-mode connected regressions/provenance/growth/check plus same
  independent SOURCE review close this packet. Any red first receives a
  causal failure tree and smallest in-scope repair, not another permission
  pause. New architectural scope cannot be inferred from this standing grant.

Execution closure: same Heisenberg SOURCE0/0/0 Approve, local scores9/9/9/9.
434 hashes/9paths/425 unchanged;31 gates and40 module repeats/160 actual test
executions green. Final source5364/859 (Session/lifecycle); query family31-line
pure Move after focusedGREEN preserves the nominal methods and exact body.
Initial field/rustfmt changes transiently exceeded the5380 ratchet; no increase
was accepted, and the final inventory is5364. sequence/review.json own that
feedback and exact limits. Reviewer confirmed the full source snapshot and
caller classification; no Reviewer-run tests. New Layout/Reserve stays ordinary
Supervisor Reducer, not carrying RejectedEvent: outer Peer OPEN input is consumed
with no exact-input retry promise. No postcommit flow/ID/Target loss was found;
ATTACH recovery has no node preparation. Full OPEN/retry dispositions stay51B.
Preliminary320 repeat executions was a doubled tally; actual6*20+2*20 is160.

This closes neither aggregate matrix4 nor five51B. Supervisor receipt/replay,
Target storage, progress ordered scan, aggregate peak/call charges and full
P3/Case C/R7/Task4 remain mandatory. Public wrapper destruction, whole-root
zero, external/TUN/WAN/throughput/commit/push are not accepted or authorized here.

### 51L. Original Supervisor nodes and prepared receipt candidates

M1/M2 preparatory Moves are SOURCE0/0/0 accepted by the same Heisenberg.
M1 moved seven original DATA/quarantine/ACK tests, only unindenting the487-line
block: Supervisor5409->4924, private test493,435 hashes/3paths/432 unchanged,
17 green commands and exact whole-parent inverse. M2 moves original snapshot,
test correlation and receipt consumption to the existing driver_input Module,
and the complete TCP replay-permit impl to private tcp/uplink_replay.rs.
436 hashes/6paths/430 unchanged,21 green commands; Supervisor4849/TCP2766,
driver633/observation264/newTCP72. Four whole-file inverses match after exactly
one relative-visibility normalization and the documented first-line doc change.
The initial E0624 exposed pub(super)'s changed ancestor; owned_upstream scope
was restored, not widened. No behavior/receipt order changed. Evidence:
/tmp/mini-vpn-p3-supervisor.73tG3G/{move,implementation-move}-{plan,source,gates,review}.json.

M3 implementation is now locally accepted by the same Heisenberg, final
SOURCE0/0/0, Module/Clean/System/DDIA9/9/9/9.439 hashes/11paths/428 unchanged,
21 final gates/40 module repeats/240 executions; behavior-*.json in the same
artifact directory and the latest existing local-result entry own evidence.
Supervisor4834 exact ratchet; driver1169, TCP replay84/cold159, error709;
new leaves621/398. Aggregate matrix4 remains open. The actual path remains
OwnerTarget's original gate -> Supervisor DATA admission -> original Model ->
root-held receipt journal -> original TCP permit release. First falsifier:
real queued Owner DATA, node preparation succeeds, scratch Reserve fails;
original input/permits/model/old storage must remain exact. No worker or fake
success port is introduced.

- Keep the complete DATA/receipt transaction in driver_input. Use a concrete
  native ReplayExtentKey AVL with original Staging/Replay/Vacant nodes. Do not
  migrate the already accepted receiver/Session trees to a generic map kernel;
  only the existing exact allocation primitive is shared.
- K_r is the Model local-send replay segment limit. H counts actual replay
  plus quarantine/current nodes once, A=H+vacancies. Node demand peak is
  max(A,H+1), bounded by checked K_r+1 and actual Node Layout. K_r+1 is a new
  explicit admission bound, not a proof that faults allow only one quarantine.
  Released-but-retained nodes remain H. Caller/error/inbox input is separate.
- Before splitting DATA, prepare a missing node and clipped replacement
  candidate scratch together. Check actual old+new simultaneous backing;
  reject/abandon leaves original free pool, scratch/prefix and input unchanged.
  Do not grow scratch by one each turn or consume the effect-only fault hook.
  After admission, original Owner/Legacy retention policies still apply;
  this does not promise rollback of an already admitted reducer turn.
- Healthy receipt entry requires no unfinished scratch prefix and sufficient
  capacity; no ACK/terminal/expiry reserve. Poisoned DATA preparation must
  preserve an existing prefix. Scalar candidate keys are not resource owners.
  Reuse original Legacy effects Vec instead of allocating a second visible Vec.
- ACK traverses original nodes in key order, borrowed through every permit
  callback, and pushes completed scalar candidates into prepared storage.
  Only an entirely successful pass removes released candidates. Error/unwind
  retains original nodes/current receipt/prefix. Terminal collects into the
  same backing then releases/removes in order. Preserve O(R+k log R).
- Expiry first borrows and releases original replay ownership using the real
  SessionExpired receipt. Only complete success permits linear tree recycle;
  no early clear, O(RlogR) clear or removal of unrelated quarantine. Cold audit
  must distinguish actual permits, metadata, nodes, and retained scratch
  capacity even at length zero. Shared payload views are not additive backing.
- Legacy/Client outer require_healthy gates remain where they are. Owner DATA
  is specially routed before that check. Pre-admission exact-return applies
  only to inputs reaching the storage seam; a poisoned Legacy outer refusal
  is not a carrying-retry claim. Use the actual Owner gate for that tracer.

Behavior scope: supervisor.rs nominal storage/error anchors (no growth above
4849), driver_input.rs, tcp/uplink_replay.rs, tcp/cold_inventory.rs, original
owner_target/error_inventory.rs, moved Supervisor replay tests, and original
owner_data_retention.rs with private owner_data_retention/storage.rs. Private
driver_input/tests/{mod.rs,storage.rs} may test concrete node structure.
Production/test/index limits1500/800/300 remain. Integrator alone ratchets
shrinks. Do not write an oversized draft; measure the complete formatted shape.
Accepted local evidence includes original-input capacity/Layout/Reserve, intermediate
preparation refusal, multi-quarantine, reuse/no-release ACK, nonempty-prefix
unwind, actual permit-wake retention, terminal/expiry and bounded node Drop,
both-mode connected/provenance/growth/check gates and same independent review.
New red gets causal diagnosis and bounded repair under standing P3 authority,
not another routine permission prompt. Matrix4 Target/progress/aggregate and
all five51B, P3/CaseC/R7/Task4 remain mandatory and incomplete.
Independent-ledger and corrupted-cache tests are defensive, not valid production
over-allocation or giant-session acceptance. The two fixture errors (post-OPEN
ledger fork and RESET enqueue wake) were repaired test-only, with original
failure records retained. No production gate changed. Next complete owner is
Target buffers/maps/addresses/returns, followed by progress ordered scans and
the phased aggregate peak/call-count proof.

### 51M. Original Target storage — executable characterization

Matrix4 remains mandatory. First freeze is test-only: target.rs may add one
private retained_backing declaration inside its existing tests Module
(1497->1498), with the new target/tests/retained_backing.rs leaf below800.
No production behavior, visibility, storage/interface, governance or unrelated
source changes belong to this characterization. Actual MemoryTarget open/write/
take_written/cold_inventory form the loop; inspect its original private backing
without a new getter. Two flows each write/drain eight bytes against live G=8.
Predict logical summed/stored bytes0 while both deques retain at least8 capacity
each; returned Bytes remain separate caller-owned values. Direct GREEN is
honest characterization, not a fabricated RED or a complete Target repair.

Prewrite current standalone governance and Target baseline, then one exact test,
related default/all-feature Target and OwnerTarget regressions, provenance/growth,
scoped fmt/diff/check and same independent Reviewer. Diagnose any new red before
another repair/test; standing P3 repair authority applies. This selects the
complete original Target storage work next: actual flow/tombstone nodes, address
backing, read/write/directive buffers, returned buffers and readiness coupling.
Characterization is now SOURCE0/0/0 approved by the same Heisenberg; actual
exact1/1, both-mode Target11/OwnerTarget185/Knife220/repository20/growth7/check
and scoped fmt/diff/strict standalone7 pass.440 hashes/2 paths/438 unchanged;
parent1498/new leaf58. /tmp/mini-vpn-p3-target.z6G5fA/characterization-*.json
owns source and19 commands (including baselines). No behavior fix is claimed.

Preparatory Move is now executed and SOURCE0/0/0 accepted: only target.rs
and existing target/memory_io.rs changed. The complete208-line inherent impl
MemoryTarget (new, snapshot, push_write_directive, terminalize, feed_read,
finish_read, take_written) moved unchanged beside the original TargetIo impl. Retain
nominal storage/config/types in parent, same six pub(crate) entries and private
terminalize (its only two callers are already child-local); no caller/Interface
changes. Update child ownership docs separately from the byte-exact impl.
Actual parent1289/child918, both below1500, no ratchet edit. Exact208-line hash
bf41a65819f2ae66402e7f634eca684036c5bf61756a394df00b52846bc8cb32 and both
whole-file inverse hashes match.440 hashes/2 changed/438 unchanged against the
characterization baseline; leaf58 unchanged.16 post-Move gates and20 exact
repeats (10 each mode) pass. move-{plan,source,gates,review}.json/repeats.json
in the same artifact directory own evidence. This unifies Implementation
before behavior, not an allocation fix. Review ran no Cargo/tests/writes.

Next design exit is the complete original MemoryTarget transaction, entered
through actual TargetIo calls from OwnerTarget, plus the existing feed/drain
operations. Source audit identifies three concrete obligations: prepare
growth before directive consumption; preserve drained outputs through the
controlled notification's unwind; prepare original OPEN/address/map and
live-to-joined storage before transfer. memory_ready's bidirectional map/slot
checks remain coupled. Retained empty capacity, old+new replacement, returned
storage and address ownership all need actual requested Layout/phase accounting.
Success publication, refusal before mutation, pressure, cancelled observation,
unknown unwind and terminal/expiry/join must share that one settlement model.
Do not equate source audit with an executed panic tracer. The next real test
must exercise original Target I/O and real Waker/actual allocation refusal,
then retained custody/counters, not a synthetic metadata-only helper.
Main's bounded current-storage assessment (not Reviewer or full-P3 score):
Module/Clean/System/DDIA8/9/6/6.10 requires complete storage/return transactions,
actual bounded peak proof and deterministic lifecycle tests plus independent
review. No new ADR/sidecar or P4 wiring is a prerequisite to that work.

First behavior falsifier is now frozen, test-only in existing
target/tests/retained_backing.rs: actual feed eight bytes, register a real
common Waker that panics, read through TargetIo and catch only in the test.
Expected runtime RED is original cold summed0 versus retained8, not a missing
field/method. Require same-flow reentry refusal, unrelated B I/O and explicit
reset exact read8/written0 settlement. New production names are not referenced
before this first RED. It executed with the exact controlled notification
panic followed by cold0-versus8 runtime RED; no compile/fixture failure.
No new route, fake port, custom production panic hook,
sleep, second Worker or global fault model. The approved limited plan below
has now been implemented and independently accepted; overall Target storage
remains mandatory. Same Reviewer approved the original limited design0/0/0,
Clean9/System8/DDIA8 before implementation. Allowed behavior paths: target.rs
(private flow output state/errors), memory_io.rs (complete original return,
cold/guard/terminal paths), memory_ready.rs::prepare (Terminal shared release
must count pending), owner_target/error_inventory.rs (exhaustive value-only
cause audit), and existing retained_backing test leaf. Limits1500/800 remain.
Original flow owns Idle|Publishing{direction,Bytes}; prepare exact output
backing and all fallible checks before drain. Keep pending charged in original
global/per-flow bytes, cold sums queues+pending once. After publication returns,
finish counters/transfer without allocation/callback/fallible work. Preserve
attempt-counter/error-priority policy. Reject ordinary same-flow reentry;
unrelated flows retain remaining G. Ordinary AlreadyTerminal is nondiscarding;
original expiry settles Graceful Written pending. No catch_unwind in production.
Use the existing exact allocator with a separate Target fault domain; count
actual local source-capacity+output-copy overlap. Pinned bytes1.11.1's exact
len=capacity Vec conversion owns a boxed slice without a Shared allocation;
conversion still occurs before commit. This is not full Target/Root peak proof.
Required following traces: partial Written/residual, exact Reserve rollback,
Graceful cancel/expiry, real B waiter->expiry notification->successful write,
normal/zero/allocation reuse and original error ordering. These now executed:
same Heisenberg final SOURCE0/0/0 Approve,440 hashes/five paths/435 unchanged.
Exact lines target1370/memory_io980/readiness534/error audit712/test470;
all within normal limits, no governance edit. Five new tests plus the prior
backing characterization run through real I/O and actual Waker callbacks.
Written count3 and8 prove partial residual and pending-only Graceful expiry;
the latter selects/wakes B's actual waiting writer, then B writes successfully.
Reserve in both directions preserves source pointer/capacity/content and cold
inventory, with original attempt counters. Zero/Unknown/counter errors remain
before allocation; empty drain does not consume the Target allocation fault.
Partial Read preserves the same Bytes address through repeated rejection and
cancel preflight failure, then releases exact read6/written2/directive state.
One unexpected test red expected the wrong repeated-cancel variant; original
AlreadyCancelled was retained and only the test corrected. One subsequent
assert layout fmt red was mechanically fixed. Production had no second repair.
20 final commands: both Target16/OwnerTarget185/Supervisor54/C67, Knife220,
repository20/growth7/libcheck, scopedfmt/diff, strict standalone compile/run7.
20 module repeats (10 each mode) execute120 tests, all green. Warnings match
the prior baseline. output-{source,sequence,gates,repeats,review}.json under
/tmp/mini-vpn-p3-target.z6G5fA/ own evidence; no globalfmt/full-library claim.
Only the original output-return transaction is accepted, not full Target.

Following the output slice, original write/feed_read/push_write_directive
growth preparation is now implemented and accepted. Same independent review
permitted TDD with the following same-scope condition. Initial design review0/1/0 identified the original
feed owned-Bytes destructor before accounting/publication. Copy candidates
alone do not close that edge: iterate bytes by borrow and retain the original
Bytes until queue/count/high-water/publication finish. A real Bytes::from_owner
Drop-panic tracer must prove copied data and stored/summed accounting agree.
No claim that the input survives its own Drop or is returned on feed Err.
That condition is incorporated; no extra path or design sidecar is needed.
Scope: memory_io private Copy-deque preparation and those three real call sites;
target.rs only generalizes the existing Storage error display wording;
existing retained_backing leaf owns tests. No new path/Interface/governance or
map/address/caller change. Parent1370 net-zero, child expected<1150/leaf<800.
Let M=min(P,G); each byte-direction capacity remains <=M, directive capacity
<=D. Shared next_capacity and exact allocation primitive, Target-specific
buffer fault domain independent of accepted output/effect tests. First=min(8,
maximum), checked capped doubling; reuse returns no candidate/allocation.
Prepare a Copy-only replacement from the borrowed original; all old count,
readiness/completion and Layout/Reserve checks precede installation and the
first directive pop. Commit uses capacity-proven single pushes, no extend.
Keep original attempt/error/Zero/WouldBlock semantics; feed's input is still
consumed on Err under the existing API, not an invented carrying return.
First falsifier: real write with AcceptAtMost directive and allocation refusal,
only missing buffer-fault seam is expected compile RED; no fixture-error RED.
Then feed/directive exact refusal, wrapped order, clamped growth/reuse and
terminal release, one test/implementation step at a time. Both-mode connected,
provenance/growth/scoped gates and same SOURCE remain required. Design exit:
all three original sites prepare actual requested backing before semantic
commit. Retained byte backing <=2*F*M, directive backing <=F*D*sizeof(value),
one growth overlaps old plus replacement. These local maxima are NOT the
full Target/Root allocation peak; maps/addresses/output/ready and aggregate
remain mandatory. No extra sidecar or pure Move is needed for this candidate.

Final same Heisenberg SOURCE0/0/0 Approve closes only these three buffer sites.
440 hashes/three changed/437 unchanged; target1370 (two display strings only),
memory_io1074, leaf732, no governance edit. Removing the four new tests restores
the accepted470-line leaf byte-exact. Original output function/fault domain
is unchanged. Buffer candidate handles Copy values, exact old/new Layout,
clamped doubling and no-scan/no-allocation reuse; commit is single-item push.
write's missing BUFFER hook is compile feedback, followed by actual wrapped
8->9/16 refusal/retry and cross-flow reuse evidence. Feed's real owned Drop
produced summed4/stored0 runtime RED; borrowed input held through publication
then explicit Drop GREENed. Directive's actual Ok-vs-Reserve runtime RED
GREENed, preserving wrapped FIFO and exact9 capacity. Feed Reserve/retry
directly GREENed with caller-held clone explicitly distinguished from Err return.
20 final commands pass: both Target20/OwnerTarget185/Supervisor54/C67/Knife220/
repository20/growth7/libcheck, scopedfmt/diff and strict standalone compile/run7.
20 repeated modules at10 tests each total200 executions, all green. Warning
headings match accepted output; final440 rehash has no drift. No unexpected
Rust failure/second repair in this packet. Reviewer ran no tests or writes.
buffer-{baseline,source,sequence,gates,repeats,source-review,review}.json in
/tmp/mini-vpn-p3-target.z6G5fA/ own evidence. Local scores9/9/9/9 do not close
full Target/aggregate. Next select the real OPEN/live->joined/retire node and
address-owning transaction with actual Layout and readiness correspondence;
preserve borrowed-input/error/monotonic-ID contracts. Shared TargetAddr has no
domain-length bound while protocol validates253; prove the actual caller bound
before treating253 as a storage witness. No guessed std-node or new generic
container-only wrapper; progress and full51B remain mandatory thereafter.

The following address packet is now final SOURCE-approved by same Heisenberg
P0/P1/P2=0/0/0. Only target.rs private address_storage
test route, memory_io private address preparation/one OPEN call, and new
target/tests/address_storage.rs (<800); parent/child remain <1500. Prepare
after old opened-count/readiness checks, before highest-ID or map mutation.
IP copies by value; Domain checks actual caller-capacity plus exact length,
allocates exact bytes with existing allocator, copies within capacity and uses
checked String::from_utf8 before commit. Independent ADDRESS test fault;
empty/IP do not consume it. Preserve raw arbitrary String/port/IP semantics,
all old error priorities and full-capacity monotonic-ID consumption. Test actual
OPEN refusal/retry, caller identity, exact destination backing, join's unchanged
single address move, retirement, and unrelated I/O. First missing-hook E0425 is
compile feedback only, not runtime RED. No new unsafe or shared Target bound.
Session effects::peer_frame validates Record before receive_open and the sole
PeerOpenRequested -> observed_turn Target call; protocol validates253. This
reachable-input fact is not a universal raw/preloaded Target or aggregate bound.
Node allocation/live->joined transaction and complete backing peak remain next
mandatory work. This limited slice does not claim all OPEN allocation is closed.
Actual lines1371/1108/new leaf206;441 hashes/three paths/438 unchanged, no debt
edit. First compile RED onlyE0425 missing ADDRESS hook; three actual tests each
GREEN. Caller4096-cap Unicode stays untouched on Reserve, retry owns exact
length, join keeps that same new allocation. Raw900-byte Unicode/scoped IPv6/
port0/empty semantics and no-allocation hook preservation pass. Existing
duplicate/conflict/stale/full/counter order passes; deliberately inconsistent
readiness fixture proves defensive priority only, not natural reachability.
20 final gates: both Target23/OwnerTarget185/Supervisor54/Controller67/Knife220/
repository20/growth7/check, scopedfmt/diff and strict standalone compile/run7.
20 repeated modules at3tests total60 executions green; no new warning headings.
Both original source inverses are exact, accepted output/buffers unchanged.
address-{baseline,source,sequence,gates,repeats,source-review,review}.json in
/tmp/mini-vpn-p3-target.z6G5fA/ owns evidence. No unexpected Rust repair here.

Pure Move is now executed and same-Reviewer SOURCE-approved0/0/0. Only memory_io.rs and new
private memory_io/flow_io.rs: move its complete737-line TargetIo impl unchanged
(SHA2564ebd4eab27c532190fc934ae8171b6645c9b065e99753291bc58b0912b7a0610).
Parent retains inherent impl/allocation helpers/faults; child uses ancestor
private items directly. No visibility/re-export/forwarding/caller/body change;
compact ownership docs and one private declaration only. Actual372/745,
no ratchet. Before/after Target23 and related/provenance/growth/scoped
checks plus exact inverse pass. One owning native
Node Layout with Live/Joined/Vacant and two ordered roots was the selected next
direction, now accepted in the native transaction below: checked F+Z/resident+prepared peak,
untouched free custody on refusal, same-node join/retire, bidirectional readiness,
callback-after-state publication and bounded iterative vacancy Drop remain
required in its separately frozen real TDD. No Index shim or generic map facade.
Final442 hashes/two paths/440 unchanged, body737 and entire parent inverse
byte-exact.20 final commands: both Target23/OwnerTarget185/Supervisor54/C67/
Knife220/repository20/growth7/check, two-file scopedfmt/diff and strict standalone
compile/run7.20 Target-module repeats (10 each mode) execute460 tests, all pass.
Warnings equal accepted address baseline; no unexpected red or repair. Same
Reviewer verified source and Main's artifacts without testing/writing.
node-move-{baseline,source,focused,gates,repeats,review}.json in the existing
Target artifact directory owns evidence. Native behavior closure follows.

Native-node implementation is now same-Heisenberg SOURCE-approved0/0/0 at its
local boundary. Seven Rust paths: target.rs (private storage/config layout/test route and
mechanical old observations), memory_io.rs, memory_io/flow_io.rs, memory_ready.rs,
tests/retained_backing.rs, tests/address_storage.rs and new tests/node_storage.rs.
No other Rust/governance/caller changes; normal1500/800 limits remain hard.
Original TargetIo owns checked F+Z actual Node Layout, Live/Joined/Vacant,
two ordered native roots and an iteratively dropped free chain. OPEN preflight
retains old priority; node/address preparation precedes highest/tree/count/ready
commit. A vacancy stays linked until infallible install. join moves the same
whole Box; retire publishes Vacant before shared notification. Cold checks
sorting/phase/height/balance/counts/free shape/live-joined disjointness and the
existing bidirectional live/readiness relationship, without allocation.
First real tracer node_reserve_preserves_open_then_original_join_and_retire_reuse_backing
uses F2/Z1, actual allocation refusal/retry, terminal capacity, original address
identity and vacancy reuse followed by fresh-node refusal. Only the missing
associated allocation fault seam is expected compile feedback, not runtime RED.
Follow with Box identity/rotation/two-child removal, checked config boundaries,
new-candidate and vacancy address refusal, real notification unwind/reentry and
defensive cold mismatch. Same independent SOURCE, both-mode connected gates,
repeats, provenance/growth/scoped checks remain required. Diagnose before repair
under standing P3 authority. Full Target backing/aggregate and51B stay open.

Final native closure:443 hashes/seven paths/436 unchanged; exact lines
target1401/memory_io371/flow_io1168/readiness534, tests798/215/new430.
20 final gates pass: both Target31/OwnerTarget185/Supervisor54/C67/Knife220/
repository20/growth7/check, scopedfmt/diff/strict standalone7. Eight node tests
repeat10 times EACH mode,160 executions green. E0599 was only missing-hook
compile feedback; all actual node/refusal/identity/callback/layout tests GREEN.
8192 real retired nodes release on128KiB stack. Same SOURCE0/0/0, local9/9/9/9.
Node cold walks are linear; existing readiness lookups remain in whole-cold
cost. No full Target peak/aggregate closure or normal-limit exception.
native-{baseline,source,sequence,source-review,gates,repeats,review}.json in the
same Target artifact directory own evidence. Supplemental evidence rerun first
rejected duplicated quiet flags before tests; corrected bounded capture reran
both Knife220 successfully, retained151 existing warnings. No Rust repair.

Accepted requested-Target-backing packet: target.rs, memory_io.rs, flow_io.rs,
memory_ready.rs, existing node_storage.rs and new tests/storage_peak.rs only.
Same Review found two plan P1s, now incorporated before implementation: pure
combined address/borrowed-overlap check MUST precede actual node allocation;
readiness construction must use existing exact allocator/single pushes before
Vec-to-Box, preserving its unrecoverable allocator policy and separate faults.
No extra unsafe, trait port, callback facade or architecture sidecar.
Checked conservative nonaddress peak contains one MemoryTarget header, actual
(F+Z) Node Layout, readiness backing excluding embedded headers,2FM byte
backing,FD directive backing,output pending+preparation<=min(G,FM), and ONE
replacement branch (byte<=M or directive<=D). M=min(P,G). Compare complete
aligned Layout extension for each branch, not merely raw byte maxima. Config
rejects before publication; withZ recomputes. Retained free nodes pay full size.
One owned-address-capacity mirror is committed with OPEN and retire, unchanged
by join, independently audited against real live/joined String capacities.
Cold also verifies actual buffer/directive capacities against static bounds.
No scan per DATA. Raw addresses retain arbitrary valid String/IP semantics;
no253 assumption. Cached Waker/opaque feed Bytes pointees and transferred
outputs belong to their supplying/subsequent owners, not this allocation
claim; future real Root phase composition must account for them once.
First actual config tracer uses F1,M=isize::MAX/4+1,G=M,D1,read/writechunk1:
earlier individual checks pass without allocation; old Ok is the expected RED,
new StorageLayoutOverflow is required. This proves the conservative gate, not
simultaneous attainability of every bound. The previous node-only extreme is
explicitly split into valid node Layout versus rejected whole configuration;
small valid/zero/addition-overflow and all other native assertions remain.
Both-mode related/provenance/growth/scoped gates, repeats and same SOURCE are
required. Full Root/progress aggregate and all51B obligations stay mandatory.

Final same-Heisenberg SOURCE0/0/0 Approve, local9/9/9/9; both plan P1s closed.
444 Rust hashes/six changed paths/438 unchanged, no governance edit. Lines:
target1405/memory_io372/flow_io1311/readiness550/node tests435/new peak237.
Config old-Ok and real near-limit OPEN reaching NODE Reserve before Layout
were separate exact runtime REDs; both minimal GREENs and all five tests pass.
Real long/raw-address and retained-buffer lifecycle, explicit defensive cold
corruption and rejected-retire custody/no-wake cover the remaining local axes.
20 final commands and10 full Target36 repeats EACH mode (720 executions) pass;
no warning delta. Existing Target artifact peak-{source,sequence,gates,repeats,
review}.json owns the evidence. No allocator metadata, opaque pointee, escaped
output, whole Root/process-memory or simultaneous-bound-attainability claim.
Progress ordered storage and phased aggregate/call charges plus all five51B
remain incomplete and mandatory; no guessed std-node Layout or generic facade.

### 51N. Original progress storage — complete Implementation first

Mandatory matrix4 prerequisite, not work deferred beyond P3. Real call chain:
checked Controller construction -> installed original progress -> committed
Executor/reducer observations -> ControllerObservationSettlement -> register/
ACK/terminal/retirement/reset. Controller retains the only progress owner.
Existing count witness is2*(F+Z), shared by pure harness scan arithmetic; it is
not a concrete allocation proof. Do not change that pure arithmetic contract.

Same Heisenberg plan SOURCE0/0/0 approves only this preparatory pure Move:
complete442-line OwnerReverseProgress impl from reverse_switch.rs to private
reverse_switch/progress.rs. State/capacity/cursor/errors remain parent. Existing
parent-private revision/scan_reverse_stall/exact_reverse_stall_candidate/
exact_entry become child pub(super), preserving effective reverse_switch scope;
other helpers stay private, existing crate methods unchanged. Move Bound imports
with the implementation; no forwarding, re-export, callbacks or caller edits.
Allowed Rust paths: that parent and new child only; only Integrator may lower
the measured parent3231 ratchet in code_growth_governance.rs.

Failure tree: body/parent inverse must match modulo four exact qualifiers,
imports/docs/private declaration; existing reverse-switch34 default/all-features
are the pre-Move characterization. Compile/privacy/provenance/new warning
failure stops promotion for causal diagnosis; no storage behavior is mixed in.
Connected Controller/capacity/provenance/growth/scopedfmt/check, repeated34 and
same independent SOURCE follow the Move. Final exact ratchet is mandatory.

This preparatory Move is now accepted: same final SOURCE0/0/0, local9/9/9/9.
Parent2789/child453/governance751; Integrator alone ratcheted3231->2789.
445 hashes/three paths/442 unchanged.442-line normalized body, whole-parent
inverse and one-entry governance inverse exact. Existing tests unchanged.
20 final commands and10 reverse-switch34 repeats EACH mode (680 executions)
green; warnings unchanged. Initial fmt separator red received only one blank
removal; expected6/7 ratchet red preceded Integrator update. Existing progress
artifact move-*.json owns the sequence/source/gates/review. No arena behavior yet.

Selected later behavior direction, NOT yet an implementation acceptance:
Before that behavior, same Heisenberg SOURCE0/0/0 approves two further pure
Moves into the same deep implementation children. Complete OwnerProgressCapacity
impl moves from reverse_switch.rs to existing progress.rs, both pub(crate)
methods unchanged. Complete cfg(test) Legacy Core impl moves from
owner_session_controller.rs to existing construction.rs; new_for_test and
output_page_capacity_for_test become child pub(super), preserving original C
scope. Exact four paths only; nominal aliases/errors/fields remain parent,
necessary imports stay cfg(test), no Layout/arena/caller/test-body change.
Before/after reverse34/C67 in both modes, normalized bodies/whole-parent
inverses, connected capacity/provenance/growth and same SOURCE are required.
Integrator alone lowers the two measured parent ratchets in this promotion.

Both subsequent pure Moves are now final SOURCE0/0/0 accepted, local9/9/9/9.
Capacity18 original lines and Legacy51 lines remain exact modulo two effective
scope qualifiers. Four full-file inverses plus the two-value governance inverse
are exact.445 hashes/five changed paths/440 unchanged; reverse2770/progress472,
C2956/construction207, governance751. Integrator alone ratcheted reverse2789->
2770 and C3007->2956.20 final gates/40 repeats/2020 executions green, warnings
unchanged. Main and Integrator each corrected a read-only inverse checker's
separator/EOF assumption; neither edited Rust to satisfy it. Existing progress
artifact construction-move-*.json owns exact evidence. No further Move is a
substitute for the next behavior: real checked-C cold construction and original
progress observation consumption must challenge the selected arena.

After those Moves, the selected behavioral direction is:
one cold exact ordered arena with N=2(F+Z) concrete Copy-metadata slots, balanced
key index and vacant chain. It costs O(N) cold initialization/resident backing,
but adds no allocator edge to postcommit observation consumption. Concrete
Layout rejection must return original checked-constructor inputs before
install_owner_ready; actual allocator failure follows the selected abort policy.
Legacy/test constructors need the same physical Layout discipline. No guessed
std-node cost, dense FlowId array, demand-node Reserve or generic map facade.
Scan remains O(logN+B), B32, key/revision cursor and no advance on classification
failure. Preserve duplicate/full/revision/generation/clock/episode ordering,
both-direction retirement preflight, untracked no-op and expiry reset semantics.
First behavioral proof must use actual checked C construction and original
observation consumption, not upgrade Legacy coexistence or a huge-count helper
test into that claim. Exact behavioral files/tracers freeze after the pure Move.
Aggregate requested backing/call charges and all five51B remain mandatory.

#### Cold arena execution freeze (2026-09-17)

Same Heisenberg bounded plan review: P0/P1/P2 0/0/0, Approve execution only,
Module/Clean/System/DDIA 8/8/8/8; arena-plan-review.json owns the review.
No further preparatory Move is required. Main is the sole feature Writer.
Production scope: reverse_switch.rs (nominal storage/error anchors, <=2770),
reverse_switch/progress.rs (whole arena/mutations/audit, <=1500),
owner_session_controller.rs (cold error anchor, <=2956), its construction.rs,
capacity.rs and cold_inventory.rs. No Root/caller/scanner expansion.
Tests: existing reverse tests/mod.rs only adapts its original corruption
fixture without growing; new private progress/tests/mod.rs and native.rs;
existing C tests/capacity.rs only adds its private progress_storage.rs route.
All normal limits apply; only Integrator edits measured grandfather shrinks.

First tracer: checked_progress_backing_survives_original_replay_reset_and_expiry.
Use real prebind OPEN/MemoryTarget feed/try_read_target_observed, retain its
original Replay page, then actual Ready reservation/new_checked and C's original
begin/finish observation transaction. Real RESET, original output/lifecycle
and timer expiry must show original backing retained and terminal entries freed.
Output owns expiry's returned page before settlement. This is BoundInactive
observation consumption with a test-plan fixture, not Active selected-read or
full Root proof. First compile RED may name only the missing measurement seam.

Keep checked(F,Z) pure count. Concrete Slot Layout and cold corruption errors
are distinct from count overflow. C constructs after grace and before Ready
install, exact-returning original inputs on error; actual allocation failure
uses the selected abort policy. Cold audit is bounded/allocation-free and
propagates through C's outer Err; never trust unvalidated height for termination.
No storage-reference getter, generic map facade, callback/await/spawn or hot
allocation. Test-only measurement returns address/capacity/count values.
Native cases cover zero/full/reuse/sparse IDs, all-direction generation preflight,
revision MAX priorities, repeated bounded scans, and damaged tree/free partitions.
Cursor uses key/revision and advances only after classification. Reset is
O(occupied), keeps backing, and restores revision0. Direct Layout rejection is
not claimed reachable through earlier real-C adapter checks without evidence.
Unexpected RED is diagnosed before an in-scope repair under standing P3 authority;
architecture expansion, growth or ownership bypass remains a hard gate.
Focused/repeated tests, connected regressions, both-mode provenance/governance,
scoped fmt/diff/check and same independent implementation review precede acceptance.
Full aggregate/call charges, all five51B and full P3 remain required.

Execution accepted: same Heisenberg final SOURCE0/0/0, local9/9/9/9, only51N.
Real checked-C original Replay->RESET->output-owned expiry retires its one local
direction without changing arena backing; it does not invent a second direction.
Seven native tests cover original AVL/free partition, sparse reuse/scan, ordered
generation/revision rejection, zero/full/reset, corruption and concrete Layout.
Review P1 array-only Layout was repaired: actual OwnerReverseProgress header and
Slot array extend/pad are checked before allocation; allocator and abort keep
array-only Layout. Native72/152 geometry has no combined-only even-N boundary,
so no huge allocation or natural-C runtime failure is claimed. Missing-helper
RED was compilation only; test-only E0382 fixed by sampling validity pre-move.

448 hashes/12 paths/436 unchanged, final source0drift. Final20 gates and40 repeats/
2180 actual executions green, warningDelta empty. reverse2768/progress877,
C2956/construction230, old tests2393/native320/newCleaf179; Integrator alone
updated reverse2770->2768/test2395->2393, governance751. Existing artifact dir
arena-repair-{source,gates,repeats}, arena-review/sequence/integrator own evidence.
No further wording review required. Next is actual phased Root aggregate/call
charges under the matrix4 obligation above; this local acceptance does not
check matrix4 or any of the five51B completion rows.

### 51O. Phased Root aggregate — first real cold entry

Mandatory matrix4 work, not full P3 acceptance. Same Heisenberg plan review
approves this finite package after one same-scope correction: ReadyTree words
must initialize by single-item push within the exact capacity, not resize_with.
ReadyTree also serves eleven Flow trees; their original tests remain required.
The existing section51 checklist and all five51B duties remain unchanged.

First phase owns the production Root::cold allocation transaction. Compute
actual generic Root header + resident[N] + Ready-owned backing + one boxed
OwnedNotified before the first resident allocation. Ready computes only its
slots, four actual tree word arrays/fixed WORD_BITS directories and two Arc
allocations, using its original concrete Arc layout helper; no raw storage or
outer authority enters Ready. Its standalone entry adds its own inline header.
Embedded headers and transferred backing count once; Notify clone is not a
second allocation. This requested-backing Layout is not allocator metadata/RSS.

Exact Rust scope: owner_ready.rs, owner_ready/flow.rs (only arc_layout internal
visibility), owner/server_root/construction.rs, existing process_plan.rs test,
owner_ready/tests/mod.rs plus new private cold_storage.rs. No oversized file,
governance inventory, Root parent, protocol, public entry or external change.
Use the existing allocate_exact_vec primitive, without its separate effect
fault hook or new unsafe, for resident/Ready slots/tree backing. Vec failures
retain Layout/Reserve; Arc/Box allocation keeps the selected abort policy.
Root's constructor-local helper is pub(super) solely for Root-local tests.

First tracer computes a legal process plan with individually legal resident
and Ready layouts but rejected aggregate, using only bounded arithmetic search.
A cfg(test) one-shot hook immediately before the first resident allocation
prevents any huge allocation if wiring regresses. The actual cold entry must
return Layout and leave that hook armed; the next small valid call consumes it,
returns Reserve plus the original plan, and a further small call succeeds.
Verify same resident/Notify backing across empty preparation->start. The hook
is injected refusal/order evidence, not natural OOM. First expected compile
RED names only missing cold-layout helpers; then implement one real path.
Native tests independently compose actual Ready types and inspect small real
capacities. Preserve InvalidResident-before-layout precedence and all existing
generation/dirty/class/cancellation/fault behavior.

Failure tree: unexpected compile failure means feedback-loop defect, diagnose
before repair; a consumed hook on aggregate rejection disproves preflight;
excess capacity disproves exact backing; moved pointer disproves transfer;
new ownership/error/growth/provenance finding stops promotion for causal repair
under standing full-P3 authority. Focused/default+all related Root/Ready/Flow/
C/physical/Executor, provenance/growth/check, scoped formatting and same Reviewer
are required. No completion claim from a helper or empty Root alone.

Next phases still required by matrix4: original C/Executor/Session/Target and
Ready binding coexistence, physical preparation/timer, admitted residents and
runtime replacement/page/return overlap, then complete actual call charges.
Compose original local bounds through cold/control authority; never blanket
2*K, copied inventories, raw getters or per-packet whole-root scans. The bounded
C/physical mapping proceeds beside this first implementation, not as another
architecture sidecar. /tmp/mini-vpn-p3-root-peak.iroUDm owns source/test evidence.

Execution accepted: same Heisenberg final P0/P1/P2=0/0/0 Approve, local
Module/Clean/System/DDIA9/9/9/9. Root cold actual tracer and three Ready native
tests pass; initial missing-layout E0599 is compilation-only RED. A native-test
sibling-private helper call gave E0624; only test assertions changed to direct
metadata observations, without exposing that helper. Exact requested backing
and single pushes are implemented; Arc/Box abort and Vec Reserve stay distinct.
449 hashes/six Rust paths/443 unchanged, final drift0; no governance change.
22 final gates and40 repeats/1080 test executions green, no warning delta.
Ready1167/flow1130/Root construction300/process_plan494/index127/new leaf109.
source/sequence/gates/repeats/review.json in the above artifact owns evidence.

Whole-scope SOURCE decision retains aggregateP1 and selects original-Authority
composition plus fixed-source conservative requested-backing proofs, preserving
51D-N. Actual types use Layout; private upstream types require honestly labelled
upper bounds, not mirror structs or guessed node constants. Every bound must
state object geometry, maximum live count, transient overlap and actual release.
Proof profiles must bind rustc commit, target/layout, dependency source and
relevant cfg/features; mismatches invalidate the proof, never silently reuse a
constant. Another supported platform requires its own verified profile, not a
change to the cross-platform product requirement. No modeled Layout rejection
after the original transaction's first side effect; allocator-abort policy stays.

Remaining finite domains: original Executor two-map node/split peak; mailbox and
Source channel retained/candidate/recycle peaks; actor channels/oneshot/watch/
semaphores/pending map/JoinSet/task/future/output/scratch; generic Target and real
C/Ready/physical/admitted/replacement coexistence; independent actual work charges.
Original ownership moves count backing once, retained empty containers are not
zero, and future/output exclusive phases are not summed as simultaneous owners.
Target's proposed static value-only backing query stays unimplemented until the
foundation closes; it must add header-free backing without subtracting from an
already padded Layout, cover each Adapter's extra owner and feed actual C reject/
exact-return before grace allocation. No raw getter, second owner or hot-path scan.

Finite channel diagnostic uses only a temporary copy of pinned Tokio1.52.0,
actual bounded send/permit/recv/close and cfg-only original-private observation.
Whole-channel quiescence, bounded live-chain traversal, no payload/history-pointer
reads, actual allocation/free rather than reuse counters and unchanged algorithm
are required. Native debug/release normal8/8 each and20 isolated near-wrap
observations are recorded in /tmp/mini-vpn-p3-channel.C4qwtI/final-runs.json.
Near-wrap starts from a consistent fresh aligned empty state, not natural MAX
operations. Both pointer users remain stopped after the observed recycle;
complete evidence is flushed before process exit, not clean shutdown acceptance.
No product Rust/dependency/network change. Same Heisenberg diagnostic closure
is SOURCE0/0/0; original whole-scope aggregateP1 remains open. review.json owns
its accepted observation boundary; Reviewer did not run tests.

Actual-producer classification is closed by bounded source audit: checked Bound
mailbox's two lanes, TCP five lanes and Actor three lanes have serial complete-
push routes. Actor HeldInbound is not independently exported: its DELIVERED
commit stays in synchronous exclusive composition operations; replacement is
same-owner mem::replace, with no second ACK producer spawned before commit.
This is route-local proof, not a type-wide guarantee. producer-map.json records
the distinction and exact source anchors. Next finite exit selected by the same
Reviewer is the pinned-source serial-push proof including receiver concurrency,
close, retained cache/candidates and wrap. A concrete failed lemma can require
representation repair; no existing strict pre-wrap lifetime limit is proven.
Do not add a cumulative message-lifetime limit or replace every std/Tokio type.
Current checked Bound mailbox's private pair/&mut send closure is serialized;
general ingress Clone alone is not that production path. Finite tests cannot
stand in for all-interleaving/lifetime proof. Matrix4, all five51B, actual C
falsifier and combined lifecycle acceptance remain open; no new Interface or
aggregate implementation is accepted by this diagnostic.

### Historical first-path candidate (current execution/evidence is above)

The implementation target is the following real shared call chain, with the
missing connection named honestly, not implied to exist:

```text
OwnerServerRoot::poll_ready_unit (real charged selection)
  -> missing bound Owner-source owning operation
  -> C/output begin_inbound_admission / begin_service_turn
  -> checked C -> original Executor / Target / Driver
  -> inward SessionCommands -> corresponding inbox
  -> observed reducer + original output admission/continuation
  -> actual actor FIFO / retained lifecycle action
```

Existing read_source.rs31 reserves before actual Target read; its103 mixed
dequeue and152 direct reduction must be replaced on Bound service, not wrapped.
Continuation310 has real output preflight;691 currently accepts actor events,
not the missing inbox transaction. The real physical fixture in
quinn_leg_actor/tests/r7_owner_physical_recovery.rs27/184–215 already invokes
try_receive_owner_b/service_output_once against an actual FIFO. It supplies
grounded endpoint setup, not a new candidate-set or production-parent proof.
Any service logic needed from a cfg(test) adapter must become a production-shared
owning operation before it counts; do not ungate the adapter or copy its business
logic into the fixture. The initial-placeholder route is not an implementation.

Unique owners are fixed: Executor retains Flow/Target/Driver and the two cells;
C retains its one paired inbox; the existing C/output root retains entered
receipt/result/output/continuation/failure; the physical composition retains
its actual actor/FIFO/slot. Ready indexes never own these resources. A1 must
prove their actual association and transfer; a list of separate fixture values
is insufficient.

Success means the original DATA is reduced and emitted once through the actual
output destination. Rejection returns the same precommit inputs or preserves
the current entered phase. Full/output pressure retains the actual command/
page and release-driven retry. Observer cancellation leaves the same owners
and wake registrations. Source unknown remains local/sticky nonzero; root
metadata corruption is root-fatal with the original claim/error retained.
Stop closes ordinary admission and inventories actual commands, replay permits,
Target, output/actions and registrations; uncompleted physical settlement stays
owned and nonzero. Output-stop is never reported as whole-session/root zero.
If A can reach Legless/replacement/expiry, it must preserve the real phase and
reject/retain the unsupported operation without losing work; it cannot process
it through an Active-only assumption. A4 must prove that boundary, not hide it.

First test candidate: bound_active_owner_source_reaches_real_output_once.
Build a genuinely opened Flow using existing protocol machinery, feed real
MemoryTarget DATA before cold binding, invoke only the production-shared selected
operation, and observe exactly one actual output/FIFO effect and unchanged
identity/byte conservation. Expected RED is the missing complete bound
construction/source-to-inbox/owning-service connection, not a fabricated sole
getter. Validate fixture/correlation baseline first; unrelated compile errors
are feedback defects, not the predicted behavioral gap.

Next separate tracers cover Ordered Full with Control service, real dequeue
rearm, cancelled observer re-entry, actual failure/completion ownership and
stop admission/remaining inventory. Each minimum GREEN stays one causal slice;
the entire A lifecycle matrix must close before Active-path acceptance. No
fake success, changed-semantics compatibility test or uncalled wrapper counts.

Progress at this consolidation boundary: design/ordering and stable AGENTS
rules changed; production/test Rust changed zero; Cargo/lifecycle tests run
zero. Existing scripts are reused for scope/whitespace, not duplicated or
counted as functionality. The implementation blocker is A1–A4's concrete bound
operation/association/write freeze, not all future expiry/B/P4 work. Review
this substantive scope amendment once; faithful result transcription needs no
new wording-only review loop. Full P3 remains the original unchecked ledger.

Same Heisenberg approved the substantive workflow/order amendment and eight-
document consistency at0/0/0, scoped Module/Clean/System/DDIA9/9/8/8. Reviewed
joint2909 SHA a2854e8728b456c52f92f1508dd28b02e3a5c96c9f90fd3297910a0ab218a3a3.
No necessary new document stage or Module was identified. The next deliverable
is one concrete A1–A4 implementation packet, or a genuine blocking architecture
choice, not another wording-review sequence. No Rust scope or implementation
was approved. Reachable RESET/FlowFinished/error/stop/phase obligations cannot
be deferred. Independent eight-file/48-link checks, original result3168 prefix,
338 Rust/governance/HEAD/index pass. Faithful facts and final audit only may
follow without recursive review; full P3 and the two broad P1s remain open.

### A1–A4 executable candidate: ownership, calls and finite migration

2026-09-08 candidate, accepted 2026-09-09 after reviews0/3/0 ->0/1/0 ->0/0/0.
Same Heisenberg grants this finite packet's compilation/TDD under the existing
user authorization; scoped Module/Clean/System/DDIA9/9/8/8, not runtime acceptance.
Inactive activation, publication failure/unwind and outer-root exit are closed.
These branches close together in this same packet. This is the single
implementation packet required above, not another prerequisite sidecar. Names
below select new code to compile; they are not claims that these APIs exist.
The readiness/backing part belongs to this same accepted packet rather than
an unspecified later scope. Existing accepted invariants remain in
force; an implementation contradiction is evidence to repair this packet,
not permission to introduce a getter, fake Target or alternate reducer.

#### A1. One actual preparation owner, one whole resident

Keep the actual root, not a second preparation index:

```rust
// All fields private. Root/preparation effective visibility is owner-only.
struct OwnerServerPreparation<T: TargetIo> { root: OwnerServerRoot<T> }
struct OwnerControllerPreparation<T> {
    session_id: SessionId,
    executor: OwnerTargetExecutor<T>,
    clock: SessionMonotonicClock,
    plan: OwnerSessionCapacityPlan,
    limits: OwnerReadySessionLimits,
}
enum OwnerResident<T: TargetIo> {
    Current(OwnerPhysicalSessionRoot<T>),
    #[cfg(test)]
    Prototype(OwnerCommandPrototype),
}
enum ReadyVisitState<T> {
    Idle,
    Running { claim: ReadyClaim, completion: ReadyVisitCompletion },
    ConstructorQuarantined(OwnerConstructorQuarantine<T>),
}
```

The existing resident record keeps its session and first local fault, replacing
its commands field with the whole owning enum. Root retains the existing one
ReadyIndex, budgets/cursors, actual Running claim and cached park registration.
T is static; there is no dyn Target, stored service future, spawn, nursery,
second selector or duplicate command owner. Prototype is cfg(test) throughout
its nominal anchor, constructor and arm; it cannot satisfy the real tracer.

Select these complete preparation operations in server_root's private
construction child, with original Root storage remaining in server_root:

- `OwnerServerRoot::cold(plan)` prepares its actual ReadyIndex, resident Vec
  capacity and park registration, returning OwnerServerPreparation or the
  original plan and precise cause. It does not publish a Session source.
- `OwnerServerPreparation::build_checked_controller(input)` selects its next
  free metadata position and synchronously reserves and invokes new_checked.
  No ReadyIndex/reservation reference escapes this operation and no await or
  dependency poll occurs in the interval. The input is one real cold session
  preparation, not an arbitrary provider/callback argument bundle.
- Known constructor rejection decomposes the original five-field failure,
  explicitly cancels its same Cold reservation and returns the original
  input/cause. If cancellation fails, into_constructor_quarantine produces the
  total lifetime-free owning exit: root's ConstructorQuarantined stores that custody,
  original executor/clock/plan and constructor/cancellation causes before
  root-fatal publication. It never fabricates a successful return or drops
  the inputs to release the short borrow. Associated reentry remains the
  section30 operation; this first caller does not invent a parent association.
- `adopt_current_only(whole)` consumes the same already-built opaque root.
  It validates actual actor/handoff/controller/FIFO correlation through the
  physical Authority and the original binding against this same ReadyIndex.
  Resident capacity and next-slot order are preflighted before activation.
  Rejection returns this whole root and exact cause. Final metadata activation
  is the last fallible operation; after it, reserved-capacity Vec push and
  owning moves publish the resident without allocation, callback or await.
- `start(self)` checks that every occupied bound position is represented by
  its actual resident and no Cold/unassociated build is outstanding, then
  consumes the preparation into the same root. Failure returns the same
  preparation. Only the started Root has the callable polling surface; no
  caller can park the preparation between reserve and new_checked.

Sequential cold adoption is sufficient for this fixed ready vector. Do not
introduce a Vacant/Preparing parent table, replacement lookup or P4 bootstrap
task to support out-of-order construction. This is explicitly the already-
authorized P3 whole-built-root adoption, not P5 fused seed publication.
from_initial_seed stays private and retains its final-fallible output new.
The separate adoption may reject the whole already-built root; it cannot
claim to return a pre-new unbound seed. Section38's fused-publication warning
does not silently turn this distinct section51 adoption into P5.

new_checked and its failure use exactly the section30 lifetime/return shape.
Its sole consuming Executor installation operation is
`install_owner_ready(self, ready, plan)`: original Executor/reservation in;
Bound Executor and the freshly paired inbox out; precise original inputs on
every modeled rejection. Progress/witness and all pair/backing preparation
precede its final bind RMW. Only new_checked calls it. The original factory
validation follows section25's order, via a private-constructor
OwnerTargetColdPreparationGate; no remint, raw identity, detached certificate
or mutation in that validation. Existing checked construction/return symbols
have effective owned_upstream visibility, including definitions implemented
in construction.rs: use the correct pub(in ...) scope, not a child-relative
pub(super) that would exclude sibling callers.

The concrete third argument is the existing inward OwnerSessionCapacityPlan,
configuration rather than a detached proof. C first derives and retains its
full OwnerSessionCapacityWitness against that same Executor and plan, preserving
capacity error order and preparing progress before installation. Importing C's
witness back into Executor would invert that dependency. Same Reviewer's
01a0860e-d2ea-7c32-aebe-666b3bff6969 ruling accepts this concrete typing0/0/0;
actual identity/domain/limits/backing checks remain mandatory in the installer.

#### A2. One selected owning service, not exported receipt choreography

Neutral owner_ready defines the finite selected source value ControlSend,
OrderedSend, ControlInbox, OrderedInbox or OwnerContinuation. Root keeps its
actual ReadyClaim; it passes only that selected value to
`OwnerPhysicalSessionRoot::visit_ready_source`. The whole physical method owns
actor gating, C/output transaction, outcome retention and deferred publication.
It returns a value-only visited/waiting/failure disposition and next eligible
masks, never Driver, inbox, receipt, permit, raw actor or mutable controller.
The same existing Root loop owns budget admission, claim-before-call, finish,
dirty/recheck and park for Current and Prototype arms.

Inside the physical method, use its original actor's physical-parts operation
and the existing C/output ordinary/service transactions. No test or outer
Root callback implements business logic. Source-only/readiness/send work
deepens the existing recovery_service state with a finite work kind; there
is not a second entered/quarantine ledger. Ordinary receipt/write/completion
uses the original output ordinary guard. Effect continuation uses the original
drained-page permit. Existing B recovery policy is not ungated or treated as
a CurrentOnly port. All actual admission/TCP/actor guards end before deferred
outer notification on success, expected pressure/error or genuine Pending.

For local inbox service, C's checked-capacity storage gains the paired inbox
and an owning receipt phase `Vacant | Held(SessionOwnerCommand) | Unknown`.
Unknown is nonzero quarantine, never an empty replacement for a lost command.
The complete new `QuinnOwnerControllerOutputInboundTransaction::
receive_selected_and_admit` operation performs this order:

1. The existing ordinary transaction is prepared but not entered. Poll only
   the selected inbox lane with its controlled Waker and section37's exact
   primitive wrapper. Mailbox prefetch owns a receipt before capacity Notify.
2. A returned receipt moves immediately into C's Held field. Validate its
   full LocalFlow, actual source factory/permits and retained phase; capture
   time from this Controller's actual session clock. No raw receipt escapes.
3. Enter the original guard before observed reduction. A C-private held-input
   operation calls the existing observed Executor reduction and feeds the
   actual drive into existing admit_ordinary. The Root method does not expose
   separate caller-controlled begin/receive/finish operations.
4. Genuine Pending/closed-lane with no receipt restores the unentered guard;
   closed-lane is not whole-mailbox zero. Expected carrying pressure transfers
   the exact event to its real Flow pending-completion phase and settles an
   empty ordinary page, not a continuation waiting for its own inbox.
5. A non-carrying DATA error retains the actual Supervisor staging/poisoned
   state; an entered unwind stays quarantined. Original controller error and
   rejected output page remain in their existing owning ledgers. No receipt
   copy or count is substituted for either physical disposition.

Move complete SessionCommands inward to owner_target/commands.rs, including
the already-extracted visit_selected_lane, both cells, ingress/observer,
origin validation, transfer/Full/Closed/unknown, cursor, rearm and quarantine.
Source pull uses the Executor's actual selected retained Flow Driver and
preflighted transit increment; Held is installed before validation/notification.
Source read only reserves/reads/commits the actual Target result and requests
that Driver's probe. A later separately charged send-source unit pulls; another
transfers; the corresponding inbox unit reduces. No fused mixed receive.

Both old read_source mixed intake and completion.rs RESET mixed intake are
cut over for Bound, including write/Offer and HalfClose failure callers.
Legacy/prechecked old read rejects Bound before reservation/read/poll. Failed
Target work first enters the actual retained Flow completion phase; RESET
staging requests Control work but never immediately dequeues/reduces it.
Positive SinkAccepted/SinkHalfClosed and E-page admission retain their current
observed path. Actor/recovery/continuation callers use these same Bound-aware
semantics; none gets a test-only legacy fallback. Full transit validation is
LocalFlow equality, not merely flow_id equality in a carried completion.

Output category drives the original page into the exact actor FIFO through
existing service-turn/output admission. One visit drives one item, or one
existing E-bounded continuation page when that category is selected. Page
Full retains its actual frame/page; lifecycle output retains its actual opaque
action, and unavailable replacement/Legless/expiry ports remain ineligible
and owned, not live-spin or success. No new B/expiry producer is introduced.
The first test may stop its runtime claim at actual FIFO enqueue. Independent
review of the endpoint boundary found0/0/0: peer observation can use real
finite actor flush/decode as a verification tail, without a fixture reducer
or output drain. This does not prove automatic transport rearm. Any output
wait that makes this Root park must gain a real release/wake proof within A;
it cannot be deferred to P6 merely because the first test enqueues successfully.
The actual Actor release/registration operation below is part of this packet,
not a future unspecified scope request.

#### A3. Concrete readiness/backing, not a snapshot converted to Pending

Select the two required TargetIo operations already specified in the Target
readiness contract: poll_readiness(flow, Read|Write|Join, cx) and
poll_ready_notifications(cx). Their exact returns are respectively
Poll<Result<TargetReadyOutcome, TargetIoError>> and
Poll<Result<(), TargetIoError>>, with Outcome=Attempt|Failed(TargetFailure).
No default method and no readiness(&self) fallback. MemoryTarget implements
both; DropTrackedTarget, FailOnceTarget and OverAcceptingTarget delegate to
their existing actual inner MemoryTarget without changing injected I/O errors.
Old diagnostic readiness remains unchanged.

Select actual storage, all fields private:

- Owner Bound backing has F_owner fixed FlowServiceSlot positions. A position
  is Free, Live or Retiring, not Option of an owning Flow. Live/Retiring retain
  exact original Flow association (full LocalFlow where a source exists;
  original SessionFlowId plus actual no-source phase otherwise), immutable
  slot/scheduling/Flow generation, checked transit, and controlled ticket
  owners for actual dependencies (at most Read/Write/Join and Driver Control/
  Ordered). A no-source record has no fabricated Driver ticket or registration.
Target/Driver/bytes remain solely in original TargetFlowOwner/source fields.
  Store the stable backing position at that original Flow; validate both
  directions before service. A failed-OPEN record cannot fabricate a source.
- The same ReadyShared slot owns only non-owning Flow metadata: demanded/hint
  masks, revisions, credit state and eleven fixed hierarchies/cursors: two
  Driver candidates plus the nine per-Flow continuation categories5–13.
  Categories1–4 have their fixed session state. Selection touches one indexed
  candidate and uses the existing map lookup, not an F-map scan. The named
  category and per-Flow cursor advance before the selected operation returns.
- Credit is a distinct mutually exclusive Available/Blocked/RetryDue phase;
  the Read attempt hint is independent. A failed credit reserve keeps the
  hint but clears attempt eligibility. Actual ordered dequeue first retains
  its command and then requests that Flow's slot-credit reconsideration.
  Actual release-only byte/segment settlement requests the bounded credit
  pass. Own failed reservation rollback is not a release edge. A Target wake
  cannot clear this phase or manufacture replay/source permits.
- MemoryTarget owns one fixed Box<[MemoryReadySlot]> of F_target positions,
  where F_target is its actual capacity witness, not assumed equal to F_owner.
  Each slot is Free or Bound to the actual Target Flow with three cached
  optional Waker references; these optional registrations own no business
  resource. MemoryTargetFlow records its unique position. A single common
  cached Waker, checked shared-release revision/pass/cursor and two waiting
  hierarchies (shared Write and Join) complete the Adapter-owned readiness.
  No second Target bytes/directive/tombstone or Flow ownership is introduced.

MemoryTarget readiness backing is created with the actual MemoryTarget, not
silently added after Controller publication. MemoryTargetConfig::new performs
the concrete Layout checks after its existing zero/directive error ordering;
MemoryTarget::new retains its existing infallible-allocation API and allocator-
abort limitation. Existing live Target registration positions are therefore
already owned at binding. No allocation is hidden in a readiness poll, read,
callback or common candidate. Initial configuration errors are typed; global
allocator abort is not claimed to return an existing Target or constructor.

Cold binding explicitly walks at most the real F_owner population to assign
and cross-check stable positions and import actual retained work. Allocate
ticket objects only for actual occupied generations, never dummy future Flow
keys; future creation still has to reserve F before allocating its own tickets.
Free capacity is not a live ticket and Retiring cannot be reused until real
registration destruction and controlled-ticket uniqueness are established.
Old immutable ticket keys are never rewritten. Include Retiring in F, not Z.
First-A may keep an unfinished retirement nonzero; it cannot reclaim it or
declare full P3 retirement proved. Source receivers/Target registrations must
really release cached references before any completed retirement assertion.

Use typed Layout composition in the concrete owning modules, with checked
adds/multiplies/extend rather than guessed byte constants:

| Backing | Exact count envelope for this candidate |
|---|---|
| Owner physical service positions | F_owner FlowServiceSlot, including Retiring |
| Owner Flow metadata/summaries | F_owner FlowReadyMetadata; 11 * H(F_owner) words and eleven bounded level directories/cursors |
| Flow controlled ticket allocations | At most5 * F_owner actual immutable publisher allocations; Arc clone references are not additional objects |
| Session controlled ticket allocations | Target common1, factory credit1, inbox2 and capacity1; actual physical Actor Output1 prepared before resident activation; original Root Notify/park registration separately |
| Actor progress registration | One cached Output Waker in SharedReadiness and at most one in the existing JoinSet lists; both reference the same Output publisher. Original watch, JoinSet lists/task entry and transient completion references remain accounted |
| Target registration backing | F_target MemoryReadySlot; at most3 * F_target named Waker references and one common reference |
| Target shared summaries | 2 * H(F_target) words, two bounded directories plus pass/revision/cursor |
| Existing physical pair and resources | The actual two Tokio channels at M_ctl/M_data, sole observer/cache, existing source channel/permits, Target buffers/directives/maps, E output/replay backing; none becomes free because new metadata exists |

H(x) is the checked sum of successive ceil(width/usize::BITS), ending at one
word, using the existing ReadyTree algorithm. Layout checks include each real
header/element/publisher allocation (including pinned toolchain Arc control
header), not only size_of<Arc<_>> or a copied count. Cold peak adds original
live Executor/Target/backing, new pair/observer/service/tickets, Root resident/
park allocations and all simultaneously live construction scratch; scratch
cannot be omitted or added as a second final owner. Build/test the concrete
types before claiming byte bounds. No cross-platform RSS/allocator guarantee.

Initial due state follows section36 exactly: fresh empty inboxes are NOT due;
existing actual source pairs get two import probes; actual pending write/join/
completion and Target interest request their own categories; recovery is not
requested until its original carrier transfers. Common registration is a
real initial work obligation and precedes any named shared-capacity wait.
Read probe establishes Attempt/hint or registered-or-published Pending; it
does not read. One later ReadAttempt preflights credit, reserves, then calls
Target.read once. DATA/EOF requests the actual Driver lane; WouldBlock consumes
the attempt and requires a later named rearm. Errors retain their actual Flow.

Bound-but-inactive is an explicit branch of the existing slot occupancy, not
Dormant treated as an active ReadyCell. Its actual due/request/credit/revision
fields retain every successful post-bind/pre-adoption source/effect/close
edge under the same slot exclusion. Update those fields with checked revision;
do not call ReadyCell::publish, index the Session class, notify Root or poll a
dependency. Original checked callers may assemble/recover/close a real owner
here. An ordinary expected rejection cannot erase already-committed due work.
All inactive commands/physical failures remain at their actual owners; metadata
corruption/exhaustion closes the original first-fault gate and preserves them.

Activation is one inward operation under that same exclusion: validate actual
Shared/key, Bound-inactive occupancy, original unused activation right, actual
prepared owner eligibility, resident capacity/order and every required source/
class revision transition first. Read the current retained due fields there,
not a constructor snapshot. After the final shared admission RMW, perform only
the prevalidated cell/phase/word stores, ancestor sets, epoch-dirty OR and
whole-root resident moves. There is no fallible ReadyCell::publish call,
Target/Driver poll, allocation, Notify or callback after that final edge.
Inactive due either arrived before this locked read and is included, or its
producer acquires exclusion after activation and follows the active protocol.
Due-but-ineligible remains retained without a runnable bit in both cases.

Activation rejection before that edge returns the same already-built whole
root; its actual inactive due and registration charges remain. It does not
restore Cold or manufacture a new activation right. A faulted/quarantined slot
cannot activate. Late publication with the original retained generation may
request only its legally retained drain/retirement work; it cannot reopen live
DATA or erase quarantine. A mismatched generation never touches a new occupant
and yields the existing stale/closed metadata disposition. No generation is
reused merely because a late publisher was ignored.

MemoryTarget Read polls buffered DATA/undelivered EOF without consuming it.
Write's finite WouldBlock/Zero directive permits one real consuming attempt;
buffer pressure alone does not. Join also checks real terminal/buffers/directives
and Z availability. feed_read/finish_read commit then notify named Read. Actual
read/take_written/terminal buffer decrease requests the bounded shared Write
pass; real tombstone retirement requests Join. Unchanged/Absent/rollback does
not request a pass. One common unit installs/rechecks its common Waker and
notifies at most one indexed candidate's three interests, or performs one end
check. Commit cursor/request disposition before notification; dirty releases
do not reset the cursor. This implements existing Target readiness semantics,
not all-Flow wakeups or a snapshot-based synthetic Attempt.

Flow Driver/inbox/capacity callbacks remain owner-local metadata publication.
Target external publishers commit same-generation metadata/ancestors/epoch
then Notify only outside guards. Actual MemoryTarget owning mutations defer
that final Notify until the whole physical operation exits its outer guard;
an external feed Adapter must release its own guards before publishing. The
same ReadyFailureGate remains the only root admission fault. No normal
external producer may be relabeled fatal to hide a missing wake edge.

Make callback context executable in the same ReadyShared slot envelope. Select
one AtomicU8 publication word outside its metadata Mutex, with ACTIVE and
PENDING bits; there is no mirrored bool inside the Mutex. The ordinary locked
metadata/generation operation still validates and updates all actual ready
state. The atomic word exists so a poisoned-lock fault path can defer without
recovering the guard or guessing whether an actor call is active. It owns no
claim, command, capacity or second fault. Include it in the concrete slot Layout.

The complete physical visit obtains a non-Clone OwnerPublicationTurn through
its actual installed binding before entering any actor guard. Root already
holds the sole Running claim. Admission validates root first fault, actual
generation/active occupancy and absence of an existing ACTIVE interval before
atomically setting ACTIVE, preserving any PENDING bit. It returns either the
sequencing guard or typed OwnerPublicationAdmissionFailure with the exact
ReadyFault and NotEntered disposition. Poison/stale/illegal interval commits
the original root fault; the Root keeps that same claim, touches no actor/C
operation and reports fault, not a retryable source Pending. The guard holds
only metadata association, never a mutable borrow of C/Target or a new lease.

Owner-local Driver/inbox/capacity publication updates correlated metadata and
then fetch_or(PENDING); it NEVER notifies from that callback, including its
poison/corrupt/revision-failure branches. Those branches atomically record the
original first fault and dirty epoch, retain poisoned metadata and set PENDING.
In particular do not retain publish_capacity's old poison->signal shortcut in
Bound mode. Inactive publication follows the inactive branch above. Prototype
keeps its existing semantics until its separately characterized migration.

An external Target publisher updates legal active metadata, releases exclusion,
then fetch_or(PENDING). If the returned word had ACTIVE it defers; otherwise
it uses the guarded outer notifier. Normal physical finish occurs only after
all source/TCP/actor guards have returned: validate locked metadata if healthy,
then fetch_and(ENABLED, AcqRel) to end ACTIVE and take pending work,
and notify outside all guards. A publisher before this swap either contributes
PENDING or defers into it; one after the swap sees inactive and notifies itself.
ENABLED is the activation-committed bit in this same AtomicU8, not a second
atomic, healthy flag, resource or mirrored bool inside the Mutex. It starts
clear and only successful activation sets it under the same exclusion after
the final admission RMW, preserving PENDING. Healthy paths validate occupancy
and this bit bidirectionally. Poisoned fault publication records the same gate,
dirty epoch and PENDING without recovering the Mutex; only the ENABLED/ACTIVE
snapshot returned by that fetch_or permits active indexing/external Notify.
Inactive faults never index Reserved Session charges or notify. Finish and
quarantine preserve ENABLED; no retirement/reuse is implied. Same Reviewer
01a0862f-c8f6-7fa3-9b79-ac8b3843562a approved this finite encoding repair0/0/0
under existing A1–A4 authority, explicitly refining the old swap(0) wording.
Activation/finish implementation and runtime evidence are still required.

New root visit admission is single-owner, so cannot overlap this finishing
visit. No spin retry or second dependency poll is part of the swap/recheck.

Finish failure is typed OwnerPublicationFinishFailure, not NotEntered: it
contains the original ReadyFault plus the actual completed value disposition.
Physical command/receipt/output ownership remains in the same original ledgers.
Root records that completion beside the same Running claim before reporting
metadata failure; ReadyVisitCompletion is only Admitted or Completed(value),
not a resource owner. It does not normalize MailboxOwned to a returned command,
discard a rejected page, rerun reduction or call the work retryable. Even when
the metadata Mutex is poisoned, after all physical guards are gone the atomic
word can be ended and its already-committed fatal notification issued. No
into_inner/poison recovery or successful ready-cell clearing is claimed.
Normal admitted work still follows original finish semantics even when another
publisher subsequently faults the process; do not add an early skip that loses
its completed disposition. Prototype test pattern migrations must borrow the
actual claim field, never reconstruct it or change its address observations.

Two unwinds are deliberately different. Source/Target operation unwind has
no completed return; leave ACTIVE/PENDING sticky with Root's actual Running
claim and entered physical/output/cell disposition. Reentry first follows the
existing resident-local quarantine; it closes the publication interval only
after owning quarantine, without claiming settled work or disabling unrelated
sessions. No guard Drop notifies, allocates, locks or fabricates a terminal.

Shared Root Notify user-Waker unwind instead uses the controlled-wake section5
notifier guard: its Drop commits typed NotificationUnwind to the SAME first-
fault gate plus atomic dirty epoch before outer reactor WakeList cleanup.
It does not catch/swallow panic, notify again or use local source quarantine.
Root retains one actual cached OwnedNotified and at most one registered waiter;
replacement destroys the old registration before rearming. Zero waiters is
inert. Preserve the proof that inner Notify has advanced past its sole Waker
before invoking it; additional observers invalidate this choice. A notification
unwind may prevent a derived visit value from returning, but the actual command/
receipt/page and Running claim remain owned and the root-fatal disposition
forbids re-admission/retry. Prior first cause always wins.

Every raw selected receive uses only the exact primitive-local concrete
unconstrained wrapper from section37. Freeze resolved default/all-feature
Tokio1.52.0 feature/build evidence excluding the active taskdump predicate
before accepting registered Pending. No Cargo flags change. Five source and
thirteen category selectors remain real, with unused categories inactive.
Check the existing 13*F*5*T_cell and (3*F+2)*13*5*T_cell sums/products plus
the Target common 2*(F_target+1) visit bound at cold construction. E work and
actual configured reducer/replay walks remain charged separately; this is
not a one-I/O-per-category or throughput claim.

#### A4. Finite compatibility migration and execution order

##### Physical output pressure operation included in A

Select the Actor-owned operation, not an outer capacity getter:

```rust
pub(in crate::owned_upstream) fn poll_owner_output_flush(
    &mut self,
    gate: &QuinnLegCompositionGate,
    now: SimTime,
    cx: &mut Context<'_>,
) -> Poll<Result<QuinnLegActorFlushDisposition, QuinnLegActorError>>;
```

It validates actual physical incarnation/FIFO and non-transferred controller
parts, registers the closed Root's controlled Output Waker and observes the
existing sole-root completion as specified below, then executes at most one
original queue.try_flush inside the original first-failure operation.
It shares the actual implementation with try_flush, whose old result behavior
is preserved. Submitted/Idle are real results; only exact transport-capacity
pressure or ordered-delivery wait may return Pending. Closing, hard failure,
registration invariant or wrong physical ownership returns its typed error,
never waiting. Register before that one real attempt; a racing release is
retained by the Root's checked revision/dirty protocol, not another flush loop.
Temporary future cancellation does not destroy the Actor-owned registration.

The CurrentOnly output category calls this port directly, OUTSIDE an existing
actor-parts closure, so it never nests the first-failure lock. The same whole
publication interval already covers it. An output category visit selects
either one flush/registration attempt OR one existing output admission/lifecycle
drive. It does not perform both automatically and does not drain a whole page.
FIFO Full requests the next flush obligation; Submitted actually releases FIFO
space and requests a later admission/remaining-flush visit. Transport pressure/
ordered wait suspends the flush obligation until a genuine Actor edge. Idle
does not self-republish absent actual retained output/lifecycle work. Preserve
the original frame at output-drain pending front on queue rejection.

Use the Actor's existing Readiness association. Deepen its current single
Arc<AtomicU64> into one Arc<SharedReadiness> retaining that exact epoch plus a
single optional Output registration; keep the existing watch allocation and
legacy wait/current/reserved/same_source semantics. same_source compares the
same shared allocation. New Waker clone is prepared outside registration
exclusion; take/replace under it; old Waker drop and invocation occur outside.
No command/queue/permit is stored there and no per-record allocation or second
watch receiver/OwnedNotified/observer/fanout table is introduced.

Do not attach Root publication indiscriminately to all legacy inbound epoch
publishes. Add an explicit actual-output-edge operation on this same Readiness
that preserves legacy advance and then takes/invokes the one Output registration
outside all relevant guards. Every such actual edge reaches the controlled
publisher even if the old watch epoch has saturated or send_if_modified returns
false. That legacy saturating value is not Root's checked ready revision.
The Root publisher/first-fault/interval protocol is still the same one.
The Bound physical call graph must keep legacy Readiness waiters absent on
this actor: both existing wait methods borrow StartedQuinnLegActor, so their
futures must end before its consuming whole-root transfer; the opaque cursor
cannot subscribe without borrowing that actor. Existing background children
hold only Readiness senders and do not subscribe. The new closed physical
service never calls either legacy wait method or exports an actor reference.
Lock that producer/holder fact with provenance and a cold registration check;
do not let a legacy watch notification panic prevent the new Output edge while
claiming registered Root progress. If that closed holder proof is false, this
packet must contain the notifier-failure treatment before acceptance, not a
blanket claim about arbitrary watch observers. Old unbound/client wait behavior
is unchanged; the first-path claim is about this actual exclusive Bound actor.

| Actual producer | Exact release/close before Output publication |
|---|---|
| Successful queue submission | Existing queue.try_flush pops the actual frame and lowers owned_bytes; after its first-failure guard, request Output work. No leg_io behavior change is needed. |
| Non-ordered data_writer_task completion | Complete actual header/payload write, explicitly drop both message/byte TrackedPermits, then the existing readiness edge gains Output publication. |
| delivery_reader_task DELIVERED | Remove the actual pending record with its mutex guard ended, mark the exact delivery token, drop both real permits, then advance/publish Output. |
| request_cancel_after_arbitration | Commit actual Closing first; publish close before pending-inbound disposal/handoff or waiting cleanup. Preserve original cancellation ownership/order otherwise. |
| finish_fixed_child normal/error outcome | Record the actual first failure and cancellation, then publish close before child-owned stream/channel resources are dropped or root awaits handoff. Thread the same Readiness through the four existing fixed-child callers; delivery_writer gains that existing association, not a new failure domain. |
| Actor-root child-join/panic fallback | Commit its actual first-cause/structural result and cancellation, publish close before inbound_handoff/remaining cleanup waits. This does not cover the outer root itself exiting. |
| Entire outer actor-root return/error/cancel/unwind | The existing actor-owned JoinSet completion registration below supplies the independent Output edge after the task result is committed. No new Output Notify is added to ActorRootExitSignal::Drop. |

The selected Output port also owns one poll of its EXISTING nursery, not a
second join Interface/task/handle or an exported future. When observed_root is
absent, validate nursery.len()==1 and poll_join_next once with the same closed
Output Waker, outside all actor first-failure/registration guards. Empty or
multiple roots fail closed with the existing typed structural errors. A Ready
result moves immediately into the original observed_root before validation,
formatting, notification or another operation. Keep its original three forms;
change only private ObservedRootOutcome::JoinError to hold the exact Tokio
JoinError rather than allocating an Arc<str> before retention. Both this port
and the existing join use the same plain owning conversion. Original external
TaskJoin text conversion happens only after retention. Later join consumes
this same ledger, never polls again for a result already consumed here.

A retained Completed result blocks further flush with Closing (subject to
the original first-failure precedence), not Joined or inventory zero. An
ActorError/JoinError retains its exact payload and original port disposition.
The observation does not cancel, await cleanup, consume the terminal stamp,
change current actor custody, or release FIFO/controller/session authority.
Actual terminal/failed settlement remains at the original owning service.

Explicitly extend section37's primitive exception ONLY to this one concrete
stack-pinned unconstrained(poll_fn(nursery.poll_join_next)) poll. It contains
no actor transaction, queue flush, root loop or stored future. The same pinned
Tokio1.52.0 trace/taskdump exclusion must be verified in both resolved builds.
Any real self-wake remains published in the same revision/dirty protocol;
Pending cannot erase it or be read as proof that the task is still running.
One Output unit contains at most one nursery poll and one queue flush, or a
retained terminal result instead. Charge both finite primitives in T_cell;
do not silently claim one raw primitive or add an unbounded completion loop.

Source proof: Tokio1.52.0 join_set.rs:500 and idle_notified_set.rs:172/438
register/recheck under one lists mutex; completion moves the entry and takes
the outer Waker there, then invokes it after unlocking. Controlled Waker clone
and replaced-Waker Drop DO occur inside that mutex: both must remain bounded
reference operations with no allocation, business callback or lock reentry.
No arbitrary outer observer Waker is installed in this new port.
core.rs:363 and harness.rs:331/500 commit the task output/COMPLETE after normal
future destruction or captured task cancellation/panic, before waking join.
Thus outer-root exit cannot depend on its unexecuted child fallback or a late
terminal. Original ActorRootExitSignal failure/cancel/legacy behavior is unchanged.

The completion publisher uses the same metadata-before-Notify and typed
NotificationUnwind guard. Tokio's catch around wake_join is NOT delivery or
retry evidence: even if it catches an outer notification panic, the original
Root first fault/dirty state and task result must already be retained. An
ordinary actor completion/failure is resident-local, not shared Root fatal.
Runtime destruction may itself occur during outer unwind; do not promise a
non-unwinding thread or progress on a stopped runtime. Retain exact results
and nonzero custody; process abort/double-destructor panic is not recoverable.

JoinSet emptiness does not clear its cached Output Waker. Keep the existing
lists/task-entry allocation and cached/in-flight references charged through
actual destruction; no empty/noop poll, copied count or premature ticket reuse
proves retirement. No new allocation is made per poll and no new publisher
object is added. Temporary observation cancellation retains the same actor,
JoinSet registration and result. Actual Source/ticket retirement still checks
all holders; unfinished actor destruction cannot be reported as zero.

Required new exit evidence is in the same owner_output_ready test leaf: a real
loopback actor, actual transport/ordered pressure and real selected Output
registration; abort the existing test-owned root_abort, await the registered
wake without manually polling actor/join, then observe failure and exact result
retention once/repeatedly. Cover completion-before-registration and after it,
normal outer Err and captured wrapper-panic with the same actual exit wrapper/
JoinSet operation. Component exit tests are not whole-Root service evidence;
the separate bound_source Root park/dirty/dispatch tracer covers composition.
No actor/queue getter or production fault hook is introduced to wire the tests.

The outbound mpsc queue has the same outbound_messages capacity as the message
semaphore and only ActorEncodedTransport produces after taking one message
permit. A newly available message permit therefore implies room for its send;
do not add an unrelated queue-dequeue subscription or claim dequeue released
byte permits. A byte-acquisition failure after temporary message acquisition,
or another submit rollback, publishes no capacity-retry edge. Actual FIFO
ownership remains on rejection. Closing cleanup (pending.clear, uncompleted
record/receiver destruction) follows the already-published close and cannot
be relabeled permission to send again.

TrackedPermit::Drop is specifically NOT the Output notifier: its current body
updates counters/notifies zero before the _permit field is destroyed. Notifying
Root there could recheck too early and miss the later semaphore return. The
explicit writer/DELIVERED drop-then-advance sites above are the real normal
release producers. No notification under pending-delivery/first-failure/
registration exclusion and no ListenerRegistry subscription is introduced.

SharedReadiness/registration backing is prepared at real Actor construction,
before Actor publication. It has no Root registration then. The one immutable
physical Output publisher is prepared only after real actor/binding identity
is known, before whole-root resident activation; count it once in cold peak.
Registration happens during the selected visit, not as a post-publication
fallible constructor step. Exact register invariant failure retains the actor/
FIFO/result and follows existing actor failure custody. The registration's
locked body only swaps prepared controlled Waker values; no clone/drop/user
code or allocation is performed under that lock. Never recover poison or
normalize its registration to empty. Publication failure obeys the same
typed Root fault/deferred-notification contract, including close edges.

Deterministic required traces include actual FIFO Full→one flush→later admission,
real transport-byte/message release and DELIVERED rearm, release during arm,
observer cancellation/reentry, rollback producing no self-wake, and child
failure waking a pressure-parked Root BEFORE inbound handoff. A late terminal
notification alone fails that final test: waiting Root and waiting handoff
would otherwise deadlock. The first DATA/FIFO test remains unchanged in scope;
these separate traces, including outer-root completion, close physical pressure
before A acceptance.

All18 original checked calls use a real unpublished ReadyIndex that outlives
their resulting Controller/root; helper-local temporary indexes are forbidden.
The two continuation helpers borrow caller-owned indexes synchronously and
keep their original tuple results; no successful Controller contains that
lifetime. Existing fixtures add their index as the last field. Destructuring
keeps it alive: bind the index first, then rebind the owner/root so that the
actual authority drops first. Every failure still retains the complete input.

| Existing caller group | Exact finite change |
|---|---|
| Controller capacity, five calls | Drift test retries the same returned reservation; horizon collision explicitly cancels its returned Cold. Two source-layout Controllers each retain their own actual domain through the assertions. No swallowed expected Err. |
| Continuation helpers, two calls | checked_root, checked_root_with_pending_recovery and root_with_real_pending_open borrow the caller index. Three simultaneous output roots need three domains. Caller-held recovery stays outside C until the original begin_attached_recovery call. |
| Physical attach, one call | Add index to OwnedComposedRegisteredFixture only; preserve the genuinely opened Flow. cleanup.rs keeps it through owner_b/returned authority destruction. |
| Physical recovery, one call | Add index to LegacyActiveBTerminalFixture; both main and post_bind_unwind consumers retain it. establish_targets=false stays unchanged. |
| Initial acceptance, eight calls | Actual local indexes cover wrong-clock returned Controller/retry, both pending/delivered roots, accepted/terminal/joined/LeglessParts until their final disposition. Already-Bound actor rejection is not Cold cancellation. |
| CurrentOnly finish, one call | prepare remains unbound. finish borrows a persistent index; SettledCurrentOnlyFixture owns it last, and all destructures retain it. Active fixture's index outlives its root/joined observation. The new real Root path uses its preparation's checked-construction operation, not an exported index. |

Necessary test growth is handled by three finite pure co-Moves, each before
changing the moved behavior/call signatures: continuation/tests/mod.rs's16
complete tests move to transactions.rs (original399 lines); initial acceptance's
four tests plus sole bind_real_initial_terminal helper move to
initial_composition.rs (original565 lines); physical_attach's nine tests move
to scenarios.rs (original739 lines). Helpers stay at their existing Authority;
no pub helper/re-export. The three FQ routes gain their named child segment;
the original function names and test inventories are preserved. The remaining
current_only parent744 must stay below800 without copying its handshake.

Execution ledger2026-09-09: continuation's399-line block is now moved unchanged;
parent330/transactions407, both build-mode16-test baselines and regressions,
final gates and same-Reviewer0/0/0 accepted. Initial acceptance's41-line helper
and524-line tests also moved unchanged: parent603/child574, both-mode post52/52,
final gates and same-Reviewer0/0/0. The existing negative capacity source guard
reads parent plus actual child with unchanged assertions; old1168 debt entry
is removed. Physical attach's739-line block is also moved unchanged:
parent668/scenarios747, both-mode post53/53 and final gates, same Reviewer0/0/0.
Its capacity source guard likewise follows both actual files without changing
assertions, and the old1407 debt entry is removed. All three test co-Moves close;
none has changed the checked-construction calls or their lifetimes yet.
Actor's separate pre-Move cancellation baseline P1 stays open; this independent
co-Move neither authorizes its test repair nor substitutes for its failing gate.

Canonical-geometry execution2026-09-09 is separately accepted0/0/0. The section22
large-N budget-sum characterization directly passed on the original code;
the sole inward value now replaces the three outer capacity fields. ReadyIndex
borrows it, preserving both foreign Layout checks and all allocation ordering.
owner_ready55/Root848/scheduler690/process_plan123; both-mode57/57,57×10 and
final gates pass. No inventory change. The foreign Layout removal, complete
metadata Move and command/Bound wiring remain their subsequent boundaries.

Execution2026-09-09: the initial Layout review0/1/0 found missing real cold
input-return coverage and required an explicit test-axis exception. The user
subsequently confirmed exactly two new rejection/return cases in the existing
process_plan.rs, retaining its ten cases. Those cases are implemented and
same-Reviewer accepted0/0/0: leaf448, both-mode Root59/59,59×10 and final gates.
This closes that finite scope/coverage P1, not full A3 Layout or legal-layout
allocator failure. Neither inner Layout check has been removed; that remains
its separate characterized step. Independent section19 first-three recipe extraction was accepted:
eight fixed test-only methods, index85/publisher_fatal708, unchanged real causal
operations, both-mode57/57 plus57×10/final gates and same Reviewer0/0/0.
The remaining six have now completed their separate test-only extraction,
same-Reviewer0/0/0. Current index/publisher/notify/park/healthy are
135/714/187/233/355; all12 test symbols/FQs and native Wake/Drop bodies remain.
Both actual claims use test-only inherent ReadyClaim assertions with opaque
expected values, not broader production visibility or copied claim authority.
Healthy-source fixed stages preserve original per-slot resident/metadata order;
only their common assertion implementation has private slot/recovered arguments.
Publisher9/9, both-mode Root59/59,59×10 and final gates pass. Section19 recipe
preparation is complete for all9 private scenarios; outward12-scenario Move,
Layout removal, full metadata placement and Bound are not thereby complete.

The separate outward12-scenario Move is now execution-accepted2026-09-09,
same Reviewer0/0/0. All49 original top-level items retain exact bodies;
only the12 test FQs change from scheduler::tests::ready_dispatch to the
Root's tests::ready_dispatch. Root tests index2, outer leaves
540/572/120/188/167; inward index124 and leaves7/150/69/48/190 retain metadata.
The new code-aware route/unique-source tracer has exact RED->GREEN evidence;
outer12/12, both-mode Root59/59,59×10, Knife162/repository14 and final gates
pass. No production/scanner/governance edit in that Move. At that boundary,
Layout removal and metadata placement remained subsequent; actor repair scope
and real Bound/full P3 were not closed by it.

Separate Layout-removal execution2026-09-09 is now accepted0/0/0: only
scheduler.rs690->688 removes the two section15 foreign checks. Current sole
Root caller checks the same N resident Layout and successfully reserves the
actual Vec<Waker> before ReadyIndex::new; neither deleted check can first fail.
All remaining inner checks/allocation order and complete other-source hashes
are unchanged. Pre/post real cold-return tests, plan12, both-mode Root59,
59×10 and all final gates pass. This closes section17 step4, not legal-layout
allocator-failure coverage or full A3 Layout. The following step5 metadata Move
must keep actual storage checks in its constructor.

Step5 execution2026-09-09 is now accepted0/0/0. owner_ready735 contains the
complete original geometry and688-line metadata implementation; Root846 uses
one private direct owner_ready import under its existing scheduler name, not
a re-export or forwarding Module. Six recipe files are byte-identical in the
inward private test tree; seven old files are removed. Full geometry/metadata/
Root inverses and24 implementation-item hashes pass; all outer59 tests and FQs
are unchanged. Existing pub(super) operations and ReadyClaim slot/lane now use
the frozen owned_upstream common ancestor required by real production callers;
original private fields remain private. No getter or other visibility was added.
AST route/ownership tracer exact RED->GREEN, outer12/12, Root59 both modes,
59×10, Knife162/repository15 and final gates pass. No scanner/inventory edit.
The following symbol-first review2026-09-09 returns0/2/0 Request Changes on
the claim that command/35-test migration is already an independent executable
GREEN boundary. That claim is withdrawn; accepted metadata code is unchanged.
The final command placement remains required, but its complete migration must
close together with real Current/Bound GREEN unless a concrete intermediate
interface is separately accepted. No such intermediate interface is accepted.

The two open findings are finite, not a new sidecar/design phase:

1. Existing Root604–670 cold admission and masks, Root724–734 ordered
   quarantine, and the command visit's value results need one closed inward
   nominal/operation placement. A private commands child cannot be named by
   its sibling Root. Test-only Prototype cannot be the production anchor;
   owner_target9877 cannot grow to hold new anchors without an authorized
   complete implementation shrink. Parent anchors or pub(super) alone do not
   prove the full closure. Preserve the existing metadata-before-source
   quarantine order and do not import outer Root policy inward.
2. The finite Prototype list below omits capture of the original Returned Box
   address before outer operations. ready_dispatch::full_pointer and
   process_plan::returned_cold_command perform that capture today; the latter
   also checks the returned command after rearm. A unit assertion consuming an
   already supplied pointer cannot create its evidence. Freeze the exact
   test-only capture/comparison lifecycle without an arbitrary getter,
   post-operation self-comparison or weaker Vec/Box/order/observer assertions.

These findings must be resolved inside this A1–A4 packet before its coupled
GREEN; they do not grant new symbols, visibility or test writes. The resumed
user goal's finite Actor fixture repairs below have since closed; next is the
characterized Actor Move. No failing gate is excluded by this correction.

Prototype remains the selected cfg(test) parent nominal anchor with its
single private commands field. The following finite operation list is not yet
a complete signature freeze for the outer tests, as finding2 above records:
from_open_completion;
check_open_completion_admission; drive_control_to_full; visit_selected_lane;
quarantine_source; and same-point assertions full_control_is(pointer),
held_open_completion(request), fresh_ready_control, control_ready,
unknown_control(origin), control_vacant, ordered_vacant and
command_quarantined(expected). Assertions return unit; actual construction/
visit outcomes return only their original owning error or value disposition.
No generic provider, arbitrary field mutation, state getter, extra observer,
Root reference, Clone/Deref/Drop or synthesized claim is added.

The35 command characterizations move inward with their original implementation;
the12 actual Root scenarios move to server_root/tests/ready_dispatch with the
section19–20 metadata recipes and original callback/receipt/Arc lifetimes.
The original process-plan cases and three accepted characterizations remain
outer. The finite metadata recipe methods use
the already selected fixed stages, two actual borrowed claims only, and no
new universal inspection interface. Characterize canonical geometry and the
two dominated foreign Layout removals separately; then move the complete
metadata to owner_ready. Record every changed FQ before running a filter.

Execution is one packet with green-to-green boundaries, not further design
stages: frozen-baseline/provenance -> necessary pure Moves/geometry -> first
real binding tracer RED -> minimal complete Bound GREEN -> rejection/pressure/
rearm/cancel/error/stop tracers -> regressions/governance -> same Reviewer.
Existing source/provenance assertions follow actual new ownership routes;
none is removed to accommodate missing implementation. Non-predicted compiler
errors are feedback defects, not successful RED evidence. Causal in-scope
repairs use the standing user grant; a new ownership/visibility/allocation
scope contradiction must return to this packet, not be patched around.

#### Exact candidate write set and structural limits

All paths in this subsection are relative to src/owned_upstream unless stated
otherwise. This is an enumerated candidate, not a directory-wide grant. Main
is the sole overlapping feature Writer; Mencius is Integrator only for exact
shrink inventory, and the same Heisenberg independently reviews the result.
New production roots document ownership/non-ownership/invariants/entries/tests.
No new file may exceed1500 production,800 test leaf or300 test-index lines.

| Existing production file | Allowed symbols/change axis |
|---|---|
| mod.rs | One private owner_ready declaration only. |
| quinn_leg_composition/owner/server_root.rs | Complete command/metadata removal; canonical geometry; actual generic Root/preparation/resident/claim/dispatch; cfg(test) Prototype compatibility only. Root budget/claim/finish/park remains local. |
| quinn_leg_composition/owner/server_root/scheduler.rs | Characterized geometry change and exact metadata/test-recipe closure; then delete this route after inward Move. |
| owner_target.rs | Private commands/binding/service routes and storage/type anchors; whole construction implementation Move and the seven-method join/retirement Move authorized below; original Bound-aware effect/completion/retirement call sites only. No new inline tests or broad data-plane rewrite. |
| owner_target/read_source.rs; owner_target/completion.rs | Bound read/RESET isolation, exact failed work/transit and observed completion changes described in A2. |
| owner_session_controller.rs | Existing BuildError/BuildFailure anchors and impl Move; checked inbox/receipt field; new private ready-service route; existing bound shared effect/phase publication call sites. Alias and public legacy behavior unchanged. |
| owner_session_controller/construction.rs | Complete checked-build implementation/returns, selected identity/pair installation, original failure source/into_parts/Debug impl. |
| supervisor.rs | Complete three factory mint methods Move; private cold-validation operation/Executor gate import at the original Supervisor authority. User-authorized Owner observed DATA error retention below is the sole observation exception; Client/legacy and reducer semantics stay unchanged. |
| tcp.rs | Earlier complete new_bound/new_internal Move and factory comparison are accepted. User grants only the Owner outstanding completion/action error with both exact counts and the accurate Owner admission-poison error, with net-zero/shrinking separators (<=2828). Other bytes/behavior stay frozen; no new storage/getter/re-export or legacy/Client change; legacy admission_guard poison recovery stays unchanged. |
| tcp/driver_receive.rs | Selected receive wrappers plus user-authorized Owner-only actual terminal-proof/paired-source validation and same-lock admission revocation; preserve original mixed receive, public Client/TUN behavior and physical poll bounds. No bulk drain, getter/re-export, alternate producer gate or TUN terminal publication. |
| session.rs | Paired selected-inbox operation with existing prefetch/epoch/callback order; unchanged public mixed receive. |
| target.rs; target/memory_io.rs | Exact two new trait operations/value types, real MemoryTarget readiness backing/positions and corresponding named/shared release edges; old snapshot/Target I/O semantics remain. |
| quinn_leg_composition/owner/physical_session_root.rs | Opaque owner-scoped representation/necessary owning methods, original fields/constructor private; actual bound selected service/activation and retained result routing. |
| quinn_leg_composition/owner/continuation.rs | Existing ordinary/service guard additions and same recovery_service finite work-kind; no duplicate output ledger, ungated recovery policy or altered terminal arbitration. |
| quinn_leg_actor.rs | Original Readiness storage/construction; complete Readiness/try_flush/EncodedLegTransport impl Moves; selected Output registration/release/close, existing nursery observation and exact ObservedRootOutcome/old join conversion only. No terminal arbitration, listener ownership or public transport behavior rewrite. |

New private production files, each owning one complete part of the above
transaction rather than a facade:

- owner_ready.rs: complete existing metadata/geometry/selection/publication
  anchors and implementation, estimated900–1400 lines after cold extraction.
- owner_ready/cold.rs: reservation/association/bind/cancel/quarantine and
  concrete layout preparation, estimated600–1100.
- owner_ready/flow.rs: correlated Flow request/hierarchy/controlled publication
  and release/retirement implementation, estimated600–1200.
- owner_target/commands.rs: complete sole SessionCommands, estimated500–850.
- owner_target/construction.rs: complete existing five constructor methods
  (2030–2213 before edits,184 lines) with effective scopes preserved, then
  actual Unbound initialization; estimated220–300.
- owner_target/binding.rs: complete original-field identity validation and
  consuming single-pair/backing installation/exact return, estimated500–900.
- owner_target/service.rs: selected actual Flow/Target/Driver/credit/completion
  service and eligibility, estimated700–1300. It does not own a second Flow.
- owner_session_controller/ready_service.rs: checked paired receipt/source/
  observed service operations and original phase retention, estimated400–800.
- supervisor/source_factory.rs: original three mint methods plus Owner cold
  validation, estimated180–300. Declaration/storage anchors stay parent.
- tcp/factory_construction.rs: original new_bound/new_internal plus actual
  borrowed gate/origin validation, estimated150–250. No re-export.
- target/memory_ready.rs: complete named/common registration, fixed summaries
  and publication implementation, estimated500–1000.
- quinn_leg_composition/owner/server_root/construction.rs: actual preparation/
  whole-built-root adoption/failure returns, estimated350–650.
- quinn_leg_composition/owner/physical_session_root/service.rs: complete
  actor/C/output selected transaction and post-guard publication, estimated
  350–700; private phases/storage stay in the physical root.
- quinn_leg_actor/readiness.rs: complete original Readiness impl plus its same-
  source registration/actual-output-edge operation, estimated250–450. Inbound
  clock/epoch behavior remains original; Output notification is a distinct
  real-edge operation, not all-inbound fanout.
- quinn_leg_actor/output.rs: complete original EncodedLegTransport impl and
  try_flush, plus the selected registered one-frame/sole completion operation
  and private plain result conversion, estimated250–450.
  Actual actor/FIFO/transport storage stays in the parent.

Visibility is the narrow existing caller domain, not blindly pub(super) in
every child. Neutral metadata/Executor/C cross-calls close in owned_upstream;
physical/outer composition calls close in owner. Original declaration anchors
are used where callers need a name; no pub use/re-export is added. Private
child access to ancestor fields is implementation locality, not an exported
getter. Keep the complete operation together if an estimate would require a
shallow pass-through split. Actual lines, not these estimates, decide the gate.

Required additional pure Moves create growth room at the touched oversized
anchors: complete Controller BuildFailure source/into_parts and Debug impls
to construction; complete Executor five constructors to construction; complete
Supervisor three factory mint methods to source_factory; complete TCP
new_bound/new_internal to factory_construction. Bodies/callers/error order
are characterized before movement and behavior changes follow a green
boundary. C3565, Executor10061, Supervisor5772 and TCP2883 cannot grow; actual
net shrinks are reported to Integrator. Owner2062 is not a behavior target.
Target1445 stays at most1500; do not add a grandfather entry if formatting or
complete notification changes need more space.
Actor was6769 before its now-accepted pure Move: the complete72-line
EncodedLegTransport impl,80-line Readiness impl and33-line try_flush now live
in private output/readiness children with their original effective scopes.
Parent is6583; children are122/88. The exact shrink ratchet is now6583, not
permission to grow back to6769. Later same-source storage/close-producer work
must stay net-zero/shrinking at that parent or select another accepted deep Move.
The current15814-line actor test index
may only gain the new private route with a mechanical net-zero blank-line
adjustment; no test body is added there. Integrator alone lowers actual shrink.

2026-09-09 executed test-only exception under the resumed goal's self-repair
instruction: the original blocked-write cancellation, queued child-fatal and
Closing/AlreadyJoined tests now delegate only their residual terminal/FIFO/join
cleanup to private tests/ordered_write_cancel.rs. Three parent-only fixed
operations select a private expected case; the sole complete transaction
checks exact frames/independent bytes/wait, real terminal and original stamp,
returns the complete envelope on precommit rejection, then retains the actual
terminal/failure through structural join. No generic provider/callback, new
production operation, synthetic delivery or ignored result is introduced.
The existing r7_arrival queued-B-ACK test adds only its already available live
delivery wait after every time assertion and before concurrent shutdown.
Original test names, receipts, first-cause pointers, no-ACK, port-state and
peer cleanup assertions remain. The full index inverse restores15818; final
index15814/leaf136/arrival513, normal leaf limits and exact ratchet pass.
Same Reviewer code0/0/0 and actual final gates close this finite exception,
not a blanket grant to rewrite old tests or any production join semantics.
The subsequent Actor pure Move is accepted: both-mode24/24,80 repeated passes,
Root59 both modes, Knife162/repository16, final gates and same Reviewer0/0/0.
The added target-only AST tracer preserves the original four negative inbound
checks across both actual children; its sole review P2 is closed. Original
runtime tests and storage/callers are unchanged. The two command findings and
the actual coupled Current/Bound implementation remain open.

Existing test files allowed for exact migration/real delegation only:

User-authorized poison/refusal exception: tcp/tests/mod.rs may add only a
private default-path owner_terminal route with net-zero separators (<=2975).
New tcp/tests/owner_terminal.rs remains private and <=800, exercising the
actual admission mutex, Supervisor-minted factory and reservation/completion/
action/identity refusals. No fabricated origin, poison clearing, production
getter/visibility, legacy behavior change or unrelated test edits. Owner revoke
in driver_receive.rs checks lock directly before any mutation/close/notification.

User-authorized exception: owner_target/tests/r7_nested_abort.rs may add one
private default-path bound_failure declaration, and its new private child
owner_target/tests/r7_nested_abort/bound_failure.rs may exercise actual Bound
failure/retained work using the existing FailOnceTarget. Keep its implementation,
injection behavior, existing tests and visibility unchanged; child remains<=800.
This does not authorize another test destination or full C/root lifecycle scope.

2026-09-11 user-authorized test-path extension (2026-09-10 request):
add only private owner_target/tests/r7_nested_abort/bound_failure/sink.rs and
its parent's private default-path mod. First move the complete existing
read_failure_preserves_independent_sink_until_earlier_peer_terminal test
(parent489-610,122 lines) unchanged; leave fixture, FailOnceTarget and
PanicProgressOnce in place. Read-only rustfmt predicts parent679/child128,
test count10 unchanged, this FQ gains only ::sink::. No re-export/visibility,
callback/new helper Interface or governance inventory change. Characterize,
move, verify inverse/hash, focused/full/default/all-feature, fmt/provenance/
governance and same Reviewer before behavior. Then the requested child owns
incoming Write/HalfClose falsifiers and necessary local assertion adaptation.
Existing production axes/limits remain; this is not a C/root grant.
Same Heisenberg request-review01a08e91-993b-7440-8c50-b5d98f8ae638 is0/0/0,
followed by the explicit user grant. Pure Move now accepted: parent679/child128,
byte-exact122-line body and inverse, both-mode10/10 and OwnerTarget138,
Knife162/repository17/governance7, same Heisenberg SOURCE review0/0/0
(01a08efb-14f9-7d53-baa5-ff54e1311569). No behavior changed in this Move.
Real Failed first then peer DATA should expose current
PendingTargetCompletion admission; passing only that guard is insufficient:
retain actual original sink work, no further Target write or DATA ACK, until
real terminal. Separate CLOSE preserves the existing nonfinal ACK and exact
HalfClose capability. Do not substitute Open-only/TCP-refusal destinations.
The granted two tracers now close independently (same Heisenberg0/0/0 each).
Original Flow has private pending_sink: Write(actual PendingTargetWrite) or
HalfClose(actual SinkHalfClose); no second slot or public Interface. Two write
snapshots and cold write-interest stay Write-only; all duplicate checks use
whole-slot occupancy. NonWrite retry retains its exact value/error ordering;
the runtime tracer proves the preceding Failed rejection, not backup-branch
execution. Final9581/1318/229/679/377 and all listed gates are green. Existing
fixture/FailOnceTarget remain unchanged; parent test edits are only three field
references. Terminal wake/selected service/C-root obligations are not waived.

The subsequent sink-local terminal-revoke wake tracer is SOURCE accepted:
only that child grows to523, unchanged fixture/parent/production. Actual
incoming Write stays in the same Flow through terminal wake unwind; this
discharges that wake-custody falsifier only, not selected service or C/root.

- owner_session_controller/tests/capacity.rs;
  quinn_leg_composition/owner/continuation/tests/mod.rs;
  quinn_leg_actor/tests/r7_initial_acceptance_settlement.rs;
  quinn_leg_actor/tests/r7_physical_attach_permit.rs and its cleanup.rs;
  quinn_leg_actor/tests/r7_owner_physical_recovery.rs and its post_bind_unwind.rs;
  quinn_leg_composition/owner/physical_session_root/tests/current_only.rs and
  current_only/active_source.rs.
- owner_target/tests/r7_initial_bootstrap_handoff.rs,
  owner_target/tests/r7_nested_abort.rs and
  owner_target/tests/target_overacceptance.rs: the three real Target delegates;
  preserve injected behavior and current test symbols. The existing initial
  bootstrap source guard follows parent plus construction child with all four
  exact mint/decomposition counts unchanged, per the source-routing rule above.
- quinn_leg_actor/tests/mod.rs: private owner_output_ready declaration only,
  net-zero index; existing real fixture helpers and test bodies unchanged.
- The old server_root tests/mod.rs, tests/ready_scheduler.rs, its control.rs,
  capacity_registration.rs, ordered_source.rs and ordered_source/send_cells.rs:
  only the complete inward35-test move/Prototype conversion. Its process_plan.rs
  stays outer with canonical geometry field-use migration and the explicit
  2026-09-09 exception for the two cold rejection/return cases recorded above.
- Old scheduler/tests/mod.rs, tests/ready_dispatch.rs and its publisher_fatal.rs,
  publisher_fatal/notify_unwind.rs, park_fatal.rs and source_unknown.rs:
  only the complete12-scenario/metadata-recipe migration with original point
  ordering, claimed addresses and actual callback lifetimes.

Corresponding new test destinations are explicitly:

- owner_target/commands/tests/mod.rs, prototype.rs, ready_scheduler.rs,
  ready_scheduler/control.rs, capacity_registration.rs, ordered_source.rs,
  ordered_source/send_cells.rs (suffixes relative to ready_scheduler where
  applicable); no copied Root scheduler implementation.
- owner_ready/tests/mod.rs and ready_dispatch.rs, plus
  ready_dispatch/publisher_fatal.rs, publisher_fatal/notify_unwind.rs,
  park_fatal.rs and source_unknown.rs: finite metadata recipes only.
- quinn_leg_composition/owner/server_root/tests/ready_dispatch.rs plus its
  publisher_fatal.rs, publisher_fatal/notify_unwind.rs, park_fatal.rs and
  source_unknown.rs: the original complete actual Root scenarios.
- The three co-Move leaves named in A4: continuation/tests/transactions.rs;
  r7_initial_acceptance_settlement/initial_composition.rs;
  r7_physical_attach_permit/scenarios.rs, each under its original parent path.
- New focused evidence leaves owner_ready/tests/cold.rs,
  owner_ready/tests/flow.rs, owner_target/commands/tests/bound.rs,
  owner_session_controller/tests/ready_service.rs,
  target/tests/owner_readiness.rs and
  quinn_leg_composition/owner/physical_session_root/tests/current_only/bound_source.rs.
  Existing indexes gain only their private declarations.
- quinn_leg_composition/owner/server_root/tests/bound_source.rs and its private
  index declaration: only the fixed resident FIFO/flush/join test operations
  authorized in the executable-scope clarification below.
- quinn_leg_actor/tests/owner_output_ready.rs: actual registered output, permit/
  DELIVERED/close/cancellation traces, below800; reuse existing private real
  loopback fixtures without opening their Interface.

Repository provenance changes are limited to routing/unique ownership locks
for these actual moves in tests/knife16_owned_leg_provenance/continuation.rs
and tests/repository_test_module_provenance.rs plus its private child
owner_bound_service.rs. Do not expand scanner or weaken existing checks.
If continuation leaf's current limit cannot hold the lock, use the latter
dedicated provenance child rather than growing a grandfathered Knife parent.
Only Integrator may edit tests/code_growth_governance.rs, and only exact
measured shrink entries. Actor scope is now the exact Output/Readiness/close
operation above, not an unspecified future expansion. No Cargo/dependency
changes, owner_initial implementation, P4–P7/table/entrypoint, commit/push or external
run is in this first-path candidate.

#### Next actual tests and acceptance (unchecked)

Owner DATA scope correction, explicitly user-authorized on2026-09-09: retain
actual staging and return the original typed error at the original gated Owner
observed DATA entrance. Client/legacy release behavior, reducer/factory/public
Interfaces and growth limits remain unchanged. Keep the complete settlement
at Supervisor; no copied resource counter, second reducer or error-only facade.
The complete implementation may move to private supervisor/driver_input.rs
with equivalent internal scope; local test-only single-use unwind points may
exercise actual staging, bound Replay and map publication custody.
Related private tests may live in owner_target/tests/owner_data_retention.rs
through its existing private index and in supervisor/tests/owner_data_retention.rs;
use the actual Executor to enter its private observation gate, never manufacture
the gate. Any separable extraction preserves behavior first and internal scope;
only Integrator ratchets measured shrink. This does not authorize another
architecture sidecar, full-P3 acceptance, commit/push or external execution.

First tracer stays `bound_active_owner_source_reaches_real_output_once`.
Refactor only the already-green active_source fixture preparation into a
private reusable owning fixture, preserving its original test body/assertions.
No new public fixture/Driver/getter is needed: the physical descendant builds
the real prepared owner and calls the actual server preparation; original
checked C/initial/seed/CurrentOnly construction then completes before actual
whole-root adoption/start. The new test invokes real Root charged polls until
its value-only output disposition proves one actual FIFO admission.

Content/cleanup inspection uses finite cfg(test) operations implemented inside
the existing Root/physical private test trees on the actual resident. They may
assert actual FIFO inventory, perform one existing real actor flush, and borrow
the same stored CurrentOnly for original structural join; they do not return
its actor/queue/Target/Controller, perform reduction/output drain, invent a
terminal, manually wake Root or report logical authority zero. The peer really
decodes full DATA Flow/direction/offset/payload after enqueue. Keep the original
correlated attachment and exact messages/summed/stored byte observations.
This test tail does not claim automatic transport progress or full cleanup.

Expected first RED names the missing complete bound preparation/resident/
selected-service connection. A fixture import/privacy error or zero tests is
not accepted as that RED. The minimum GREEN must contain the actual complete
Target -> Driver -> cell -> paired inbox -> observed C/output -> FIFO route;
no intermediate metadata GREEN can be promoted as this path.

Then separately prove: exact same-input constructor failures/cold cancellation;
actual same-origin foreign backing rejection; Ordered Full with independent
Control; source-credit release distinct from Target wake; real inbox dequeue/
Full rearm; Root park followed by real Target feed without manual wake;
observer cancellation/reentry; exact failure RESET and completion retention;
stop closes ordinary admission while retaining all actual work. Output-Full
release/transport readiness uses the actual Actor operation and exact producer
scope frozen above. Do not call P6 or a fixture flush its implementation. First
GREEN is not full A acceptance before the separate pressure/close traces pass.

Run default and all-feature related matrices with exact nonzero counts;
repeat selected cancellation/unknown/wake traces10 times serially; preserve
existing excluded real-loopback fixture debt explicitly. Rebuild standalone
governance from current source, run Cargo governance/provenance, fmt/diff and
all-feature check, then same independent Reviewer. The design review permits
the frozen Rust sequence; implementation still needs these execution/review
gates. The full P3 ledger remains unchecked.

#### 2026-09-09 executable-scope clarification — user authorized

The already-authorized owning-fixture extraction above has executed and passed
same-Reviewer source0/0/0 plus Main's final gates; current_only754 and
active_source244 preserve the actual setup, retained resource lifetimes and
constructor/join tail. This is not the missing Bound-path GREEN.

Bounded executability review01a08579-6c95-7fa2-ad87-b6654ef55212 returned0/1/0.
The user's explicit "同意授权" now authorizes these two same-packet corrections:

1. Add quinn_leg_composition/owner/server_root/tests/bound_source.rs and its
   existing index's one private declaration. Only the already-selected actual
   resident FIFO assertions, one real flush and same-CurrentOnly structural
   join belong here. Necessary methods remain cfg(test), narrow owner-scoped;
   no public tests Module, resource reference/getter, callback or dispatcher.
   Physical-side operations still live in the physical private test tree.
2. Extend owner_target.rs's preparatory Move axis to the complete contiguous
   seven-method join/retirement closure currently at4695–4935: finish_target_flow,
   retire_target_tombstone, retire_recorded_target_tombstone, try_join_target_flow,
   retire_just_joined_tombstone, drain_pending_target_joins and expire_next_target.
   Destination is existing owner_target/completion.rs. Storage/callers stay
   parent; four parent-called entries retain equivalent pub(super) scope and
   three internal helpers stay private. Preserve scans, allocation, errors and
   deletion order. Characterize/invert the complete241-line block, then let
   Integrator ratchet actual shrink; later anchors must fit the new ceiling.

The characterized Target Move has now executed: parent9877->9635 includes the
241-line block plus its separator; completion164->411, provenance507->591.
Exact inverses, same-Reviewer source0/0/0, both-mode Target74/74x10 and final
related/provenance/governance gates pass. Integrator alone ratcheted9635.
Add the authorized Root test route with its actual fixed test operations, not
an empty scaffold. The original two command findings, full real
Current/Bound acceptance and later
P3 obligations remain unchanged; no new sidecar or temporary production
Prototype may substitute. Scope review is not Rust runtime acceptance.

Implementation checkpoint: the first Bound tracer and both actual fixed test
tails are now present. Same-file post-checked constructor extraction preserved
the original body and passed CurrentOnly6/6 before the new RED. Exact Bound
compilation now names only the missing real preparation/resident/output
connection; no runtime GREEN is claimed. Continue this accepted packet's
implementation. Same Reviewer's RED-source audit returns0/0/0, not Bound
implementation acceptance. Scope is unchanged; no further authorization is needed.

Current coupled implementation checkpoint: cold custody and original-factory
identity validation are written/source-reviewed0/0/0. Complete SessionCommands
and35 byte-identical tests moved inward; neutral work/mask values, one fresh
empty pair/observer and generic whole Current-or-cfg(test)-Prototype Root are
written. Same Reviewer source0/0/0 closes the finite placement/capture concerns
for this delta, not standalone Move GREEN or actual Bound acceptance.
Current library compilation is RED2 on missing Current service/quarantine;
full-FQ Bound exact compile is RED6 on preparation/output/service references.
No new runtime test ran. Fmt/diff/standalone governance6/6 pass; no ratchet edit.
Finish consuming checked installation and actual backing/preparation/adoption,
then complete selected readiness/receipt/output under this same packet. Current
quarantine failure must retain the real claim/completion before any Idle change.
Cold regression, lifecycle matrix, provenance and final independent review
remain mandatory. Compiler-message counts do not count remaining goal tasks.
The local-results identity/command-coupling checkpoint owns exact evidence.

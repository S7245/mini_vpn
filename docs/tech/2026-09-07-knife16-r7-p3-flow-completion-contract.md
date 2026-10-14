# P3 — Flow failure feedback and terminal settlement

Date: 2026-09-07.

Status: **PHASE/PROTOCOL ACCEPTED; JOINT BINDING FREEZE OPEN; NO RUST GRANT**.

## 1. Decision surface

This deepens the selected Flow-local completion direction in the
[construction decision](2026-09-07-knife16-r7-p3-binding-construction-phase-decision.md).
The existing Executor owns the complete failure/feedback/terminal transaction;
the existing Controller/output root owns its admission and interruption.
No extra session queue, alternate reducer, Target worker, public cutover or
second cleanup owner is introduced. Source/command/reducer/effect ownership
does not migrate into readiness metadata.

This is a source-derived phase contract, not current Rust behavior. The joint
ready-domain placement, constructor callers, guard representation, K and
physical Layout freeze is still required before implementation. Full P3,
Case C/R7/Task4 and P4–P7 remain incomplete. Autonomous causal P3 design repair
is authorized; Rust, governance, external gates and commit/push stay closed in
this document stage.

## 2. Causal counterexamples, not runtime regression claims

The [Executor](../../src/owned_upstream/owner_target.rs) at5293 rejects every
input for a pending-completion Flow. Its event mapping at5807 includes its own
LocalReset, peer RESET and TerminalGraceExpired. Removing a feedback-only
continuation alone therefore does not remove the input-exclusion cycle.

The [read transaction](../../src/owned_upstream/owner_target/read_source.rs)
at35 can fail while the reverse direction already owns pending_write. The
failed current work is not that Flow's entire retained payload. In execute_queue
at4538, Offer/HalfClose check pending_write but not pending_completion before
calling Target. A new phase must govern those effect calls as well as public
read/retry entries.

The [reducer](../../src/resumable/session.rs) at2309 commits peer RESET by
removing the live Flow and creating a terminal tombstone. A later LocalReset
fails validate_local_flow/precheck_flow_live (1916/2542) with RetiredFlow, even
after the grace tombstone expires. The [Supervisor](../../src/owned_upstream/supervisor.rs)
at1295 returns that non-pressure error without the command. RetiredFlow is
neither an owning rejection nor proof that this local feedback was settled.

Executor finish/join/expiry at5024/5132/5227 can remove the Flow record, dropping
its source and newly retained completion. Deferring only Target tombstone
retirement does not protect that record. Also, the existing
[Driver terminal operation](../../src/owned_upstream/tcp.rs) at1635 closes and
bulk-drains all three source queues. It cannot be relabeled as bounded normal
feedback routing or used to prove that a queued RESET reached the inbox.

## 3. One owning phase, with independent physical obligations

The proposed representation remains private to the existing Executor Module.
Deepen TargetFlowOwner/PendingTargetCompletion; do not make a parallel table of
failure objects. Names below describe phase payloads, not public Rust names.

| Phase | Concrete retained payload / next legal transition |
|---|---|
| Operational / no completion | Actual Target/source plus any single pending sink operation. Normal source/Target service is eligible. |
| Operational / existing Open or HalfClose completion | Preserve its exact request/result or completion event. No second operation may overwrite it; terminal input is not excluded merely by its presence. |
| Failed / Prepared | Own the exact failed current work and failure cause before staging RESET. Preserve an independent pre-existing pending sink operation. |
| Failed / StagingUnknown | Same failed work remains owned while the source call may enqueue. No repeated Target call or synthesized second RESET after unwind. |
| Failed / InTransit | Source accepted the sole RESET; actual command resides in Driver, cell, mailbox/prefetch or current reduction. The Flow holds correlation and work, not a duplicate command. |
| Failed / Rejected | Own the actual carrying-rejection command and original work after precise Z pressure. Retry reduction after genuine Z release, never source production or RESET staging. |
| TerminalSettling | Own the exact observed terminal fact and all still-unsettled source/feedback/join obligations. Work may be released only by that terminal's actual release transaction. Grace expiry may retire Z without deleting this F record. |
| Quarantined | Existing root/session failure Authority retains the precise uncertain phase, cause and physical holdings; no normal retry or clean-zero inference. |

The failed-work payload distinguishes read/probe intent from the actual
Offer+segments or HalfClose capability. The original current effect moves out
of the effect queue into this phase before its effect step is considered done.
The remaining queue and output page retain their real work; no fictitious
NeedsResume is issued merely to wait for feedback.

Replace the sink-work choice with one owning Write(offer,segments) or
HalfClose(completion) position, rather than adding an unbounded deferred list.
A failure already owning an Offer cannot also duplicate that Offer in the sink
position. A read failure may coexist with the reverse-direction position.
Reducer pending_sink_offer and pending_half_close exclusion at2879–2949 bounds
the single outstanding sink operation. Additional same-Flow Offer/HalfClose
effects are transferred there without calling the failed Target; a conflicting
second capability is an invariant failure, not another queue entry.

No completion phase by itself blocks ordinary terminal or exact feedback
admission. Global attach, actual pending effects, output-page and recovery
exclusions remain. Replacing the old blanket per-Flow predicate requires
command-aware validation at the existing Executor intake, not an
allow-while-continuing flag. Peer DATA may still consume bounded reducer
receive capacity; its sink work cannot invoke the failed Target. Unknown or
cross-Flow commands retain their real existing error semantics and custody.

Cold binding migrates existing Open/HalfClose/Failure carrying completions as
already-owned commands, not as a new RESET to send. Do not reconstruct a failed
work payload that the old synchronous path already settled. Valid existing
live/recovery inputs remain valid; cold is not pristine.

Physical presence is a separate typed choice for each domain, not an assumed
resource: SourcePresent must finish its real revoke/drain; SourceNotRetained
must preserve the exact checked Open disposition that never published that
source. TargetPresent must finish its real terminal/join; TargetNotAdmitted
preserves the actual failed Open result, not a fabricated joined value.
Current open at4462 creates a fresh source before Target.open; Failed and
AlreadyTerminal at4482–4519 do not retain that source. Failed has no Target;
AlreadyTerminal retains the actual Target terminal kind and still owes its
real join/retirement. Cold migration checks these existing exclusive fields
and the corresponding carried Open request/result as one invariant. Missing
source alone, an arbitrary None or counter=0 is not absence evidence; invalid
combinations reject with the original Executor unchanged. No Driver is rebuilt.

For such an Open completion rejected by Z, retain that original command and
request correlation. Its eventual observed Open resolution or earlier actual
terminal settles the completion once. A no-source record cannot accept
Driver-origin feedback: there was no published source. It can close without
lane polls only through the checked SourceNotRetained branch. A no-Target
record needs its actual TargetNotAdmitted disposition instead of calling join;
the AlreadyTerminal branch must not borrow this exception.

## 4. Exact feedback and earlier terminal

On the first failure, preflight phase capacity/identity and store the owning
failure payload before source staging. A normal successful send changes only
the phase to InTransit. Mailbox Full returns its actual boxed command to the
same send cell; the Flow still does not own that command. ClosedReturned and
TransferringUnknown keep their existing dispositions and cannot become zero.

Driver try_send_reset at2796 returns only FlowPortError on rejection, not a
LocalReset object. TerminalLaneFull conflates an already-admitted once-only
RESET with physical Full. Consequently it is not a generic retry signal: a
new bound failure that cannot prove fresh admission retains its work and exact
error in quarantine. Existing cold carrying RESET is migrated separately.
No snapshot flag is used to manufacture or recover an in-flight command.

On exact feedback receipt, validate full LocalFlow authority (session, Flow,
direction and authority sequence), expected ResetReason and phase before any
consuming reducer call. A live Flow's first RESET takes the existing observed
Supervisor route. Only modeled pressure returns a carrying rejection; Z Full
places that actual command in Rejected and ends this ordinary turn without an
effect continuation. This does not promise the original Box address: current
Supervisor reconstructs the same owned Control event in its rejection Box.

If peer terminal committed first, retain its actual FlowFinished payload
(TerminalGrace plus reason) when the Executor consumes that effect. Validate
TerminalGrace.local_flow against the source's full LocalFlow, not just FlowId
or a progress snapshot. Under the same entered release phase, settle the
failed work and pending sink operation against that terminal. This is terminal
abandonment, not SinkAccepted, application ACK or successful Target delivery.

The later exact RESET is consumed as a local terminal disposition under that
retained fact; do not call the already-retired reducer, fabricate a second
flow_terminal observation, create another tombstone or retransmit a local
RESET. The original peer terminal observation continues through Controller
once. A local-disposition result carries no protocol progress observation.
The terminal fact remains local until all command/source obligations settle,
even if its separate grace capability has already retired reducer Z.

Already-produced DATA/CLOSE may also outlive terminal in the independent
Ordered pipeline. They require the same full-source correlation and observed
terminal disposition, including actual DATA permit release; they are not
submitted to a retired reducer or silently lost when a record is removed.
Matching a numerical retired FlowId alone never authorizes these releases.
Staging UplinkData can physically release its payload and permits by Drop
(tcp.rs1849/2530), but that normal release occurs only inside this matching
terminal transaction. Already-bound replay continues to use Supervisor's
FlowFinished processing at1897 and release_after_reducer_terminal at2031;
neither its ownership nor quarantined replay is swept away by staging cleanup.
CLOSE has no remaining byte/segment reservation after commit_close at2553,
but remains one actual FIFO message; disposing it cannot generate a final ACK.

## 5. Bounded source retirement and actual in-flight command inventory

Select an Owner-only terminal revocation operation at the existing Driver
Authority. It validates the real FlowFinished proof and closes future source
admission under the existing port admission lock, but does not bulk-dequeue,
forge a RESET or publish a TUN terminal action. Client/TUN terminal delivery
and sealed local-abandon behavior are unchanged. Normal bound Driver lane
polls continue draining one selected command into the existing two cells.
Owner-local callbacks still obey the controlled-wake deferred notification
contract; terminal close is not exempt from its lock/unwind audit.

This port is restricted to the actual exclusive Owner source: read is the
only DATA/CLOSE producer, failure staging the only RESET producer, and there
is no split/export or SinkDelivery/HalfCloseDelivery producer. Cold/service
preflight must exclude a surviving reservation, delivery completion or extra
production borrow. A reservation internally clones a sender and owns a permit;
non-Clone Flow alone is insufficient. Generic Client paths keep their existing
different producer model and cannot invoke this proof by analogy.

An actually present source remains with TerminalSettling until both closed
physical lanes have returned drained. Empty/Pending before revocation is not that
proof. Control covers both RESET and ordinary completion queues. Each drain,
send and inbox reduction/disposition is separately budgeted; no FIFO scan or
targeted mailbox removal is allowed.
FlowPortSnapshot at911 has no queue lengths: a queued RESET or CLOSE can make
bytes/segments/actions/reservations all zero while is_quiescent is true. That
snapshot cannot replace the lane and transit proofs.

Add a checked private per-Flow in-flight ledger for commands that have left
the actual Driver and not yet reached their owning disposition. It is
accounting, not a replacement command/capability. Increment on the same
non-suspending transaction that installs the returned Driver input into its
vacant cell, before another callback/yield. Preflight overflow before polling.
Move through cell/mailbox/prefetch/current/rejected completion without another
increment; decrement exactly once at actual observed reduction or terminal
disposition. Full, cancellation, unknown send and Z rejection do not decrement.
Retained commands themselves remain the physical evidence.

The invariant is bidirectional: this ledger equals the sum of actual matching
Driver-origin commands in those owning positions. The bound pair has no raw
producer/receipt escape; new intake must carry sufficient sealed origin and
full Flow correlation to distinguish it from Target-open or actor input.
Its concrete carrier and field placement must be selected in the joint
Interface/Layout freeze, not supplied by an unchecked counter setter.
Cold preparation starts empty command transit and audits existing Flow/source
ownership; any already-carried completion is counted at its actual location.

Record deletion conjunctively requires: the checked source-presence branch
settled (real revoke/both-lane drain if present; proven no published source if
not retained); no in-flight commands or unresolved completion/work; the checked
Target-presence branch settled (real join/retirement if present; exact failed
admission if absent); and actual publisher retirement where one was installed.
These domain-specific absence proofs are not alternatives to draining an
existing resource. Unknown remains nonzero. A joined Target may release its
socket earlier; deleting its whole Executor record is a later transaction.
F includes this retiring record; Z cannot be borrowed to hide it. Grace expiry
must remain legal and cannot be blocked by the failure-completion predicate.

## 6. Existing root guard and causal release finalization

The [Controller/output root](../../src/owned_upstream/quinn_leg_composition/owner/continuation.rs)
remains the owning entry. Ordinary inbox reduction preflights the same output
and real-continuation exclusion before consuming a receipt. The Controller
owns the received command in current work before a user-code/unwind point;
the root marks its existing admission guard entered before reduction/effects.
No raw command escapes for a caller-side later apply.

Normal Pending with no receipt explicitly settles the guard unentered/vacant.
Normal precise rejection retains the actual receipt either in its legal
current/prefetch position or the matching Flow completion, never both.
The output guard explicitly settles this no-output rejection as a completed
ordinary turn; it is not an unknown Drop or a root-fatal conversion of expected
Z pressure. Genuine post-entry failures retain the existing quarantine model.
This is a new local-receipt entry, not a blanket change to actor error policy.
Actor arrival/pressure/caller integration remains under its existing frozen
contract and later actual wiring review; no new actor bypass is inferred.

Source/metadata-only work uses the existing root service Authority without
demanding that an unrelated output page be vacant. It cannot reduce a command
or execute page effects. Failure staging stores Flow custody and asks for
ordinary feedback service; it never steals a continuation permit.

Across every release-capable Executor operation, sample the existing source
factory aggregate owned bytes/segments before and after its release-only
interval (snapshot uses these actual counters at2247/2248). Release may occur
on normal Ok or recoverable Err. Finalize credit rearm and local outer wake
before the owning operation publishes either result; no post-Ok caller hook.
Actual input reduction, continuation/effects, write/completion retry, recovery,
terminal/expiry and source terminal disposition are all covered. Source-read
reservation/production is separate and is not a release-only interval.
The existing entered guard retains unknown interruption, not a new guard
ledger or a fabricated rollback. No dynamic callback wrapper is selected.

## 7. Falsifiers and remaining joint freeze

The first owning test must combine a real pending write with a read failure
and an earlier peer RESET, through the actual Controller/output/Driver/cell/
mailbox chain. It must prove no second Target call/RESET, no self-exclusion,
one actual terminal observation, exact late-feedback disposition, retained F
after Target join, and released bytes only at an observed terminal phase.

Then isolate: actual mailbox Full; Z rejection followed by another real grace
release; terminal before/after feedback; grace expiry before late feedback;
queued DATA/CLOSE after terminal; duplicate/reset-admission ambiguity; wrong
full LocalFlow; cancellation and send/reduction unwind; partial source drain;
Target join before inbox drain; cold existing rejected completion; and shutdown
without live source re-publication. No manual ready-bit injection supplies the
edge being tested. Each tracer uses the same owning Interface and one causal
RED/GREEN sequence. Quarantine does not count as a successful cleanup result.
Cold Open pressure tests separately cover Failed with neither retained source
nor Target and AlreadyTerminal with no retained source but a real Target. They
must retire the exact completion and distinguish absent from actually joined;
no synthetic lane None/Target Joined or reconstructed endpoints are permitted.

This phase adds bounded terminal drain/disposition and sink deferral obligations
to the continuation-category inventory. Their concrete selection, guard/carrier
types, K, byte work and retained physical storage must be frozen jointly with
ready-domain construction. No numerical K or TOTALBYTE follows from this table.
The two old full-map completion/join selectors and bulk Driver terminal drain
cannot remain reachable on the bound path under a renamed scheduler unit.
Full P3 still includes the static Target-open/TCP/DNS contract and ledger.

## 8. Evidence

Mencius supplied the independent source audit; Main owns the phase proposal.
Heisenberg's first independent review returned0/1/0 Request Changes: unconditional
lane/join requirements excluded valid source-less/Target-less cold Open states.
This single-file repair separates actual presence from checked absence and adds
the auditor's producer, staging/replay and zero-byte-message premises. The same
Reviewer's re-review passed P0/P1/P2=0/0/0, Approve only for phase/protocol;
scoped Module/Clean/System/DDIA8/9/7/8. He verified the302-line repaired hash,
7 links, unchanged six companion documents/338 Rust/governance/HEAD/index.
He authorized factual status/evidence and companion navigation/learning
promotion, followed by final packet consistency review. Joint/carrier/guard,
K/Layout, physical storage and Rust remain unapproved; full P3 is incomplete.
The entry snapshot is
`/tmp/mini-vpn-p3-flow-completion-cIJRri/entry.json`:1153 files/338 Rust plus
HEAD/index, checked against the last accepted construction packet. No Rust,
Cargo/test, fmt, governance, external run or commit/push occurred.
Final seven-document consistency review also passed0/0/0 Approve: packet307-line
contract,36 links and338 unchanged Rust/governance/README/HEAD/index verified;
protocol unchanged and first RC history retained. Reviewer allowed this exact
evidence transcription without another review. audit-packet.json binds that
review; audit-final.json binds the final two-file evidence transcription.

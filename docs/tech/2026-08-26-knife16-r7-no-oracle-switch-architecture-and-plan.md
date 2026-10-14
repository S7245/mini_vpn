# Knife16 R7 no-oracle switch architecture and implementation plan

Date: 2026-08-26.

Status: **ARRIVAL PREREQUISITE AND LOCAL POLICY KERNEL ACCEPTED AS
NECESSARY-ONLY; PHYSICAL R7a-d NOT YET ACCEPTED; PUBLIC TUN/WAN/M2 NOT
AUTHORIZED**.

Accepted checkpoint:
`docs/tech/2026-08-26-knife16-r7-policy-kernel-and-arrival-prerequisite-local-results.md`.

Latest architecture-only cutover and accepted test-Move evidence:
`docs/tech/2026-09-01-knife16-r7-physical-root-move-and-architecture-convergence-local-results.md`.

## Stage goal

Close the local R7a-d transaction through production-shared boundaries:

1. capture an immutable inbound arrival timestamp at complete Quinn
   transport-read admission, in one endpoint-session monotonic clock domain;
2. prove a reverse-only A DATA/application-ACK blackout does not switch without
   an authenticated B hint;
3. derive one owner hint only from reducer-owned Target-to-client outstanding
   replay and positive application-ACK age, then let the client start one
   exact-next B ATTACH;
4. reject every wrong/stale/duplicate hint without mutating the session
   snapshot; and
5. arbitrate hard failure, local ACK stall, and authenticated hint through one
   switch epoch, one pending nonce, one attach, and at most one generation.

This stage is sufficient only for deterministic local R7a-d acceptance. It is
necessary-only for Task 4, public production TUN, WAN continuity, throughput,
and M2.

## 2026-08-26 implementation checkpoint

The endpoint-session clock, atomic actor arrival envelope, semantic deadline
arbitration, stable first-fatal terminal timestamp, reducer-derived positive
application-ACK observation seam, and bounded value-only reverse-switch policy
kernel are locally accepted. Focused, repeated, all-target/all-feature,
release, provenance, vendored dependency, and independent review gates pass.

The checkpoint deliberately does **not** accept the full stage goal above. The
production client root still discards inbound observations and has no physical
B/hint/ACK-stall/arbiter scheduling category; the owner observation seam has no
production owner-controller consumer; physical B probe/hint provenance and the
single production trigger arbiter remain unwired. P1-P4 below therefore remain
implementation contracts rather than production-reachable acceptance.

## 2026-08-27 post-review architecture deepening

The first physical composition tracer exposed a missing seam rather than a
local wiring omission. `QuinnOwnerRecoverySessionComposition` owns only the B
actor plus `OwnerSessionController`; the initial A actor and its
`OwnerTargetRecoveryHandoff` remain outside it. It also retains one ordinary
output page without a drain, while caller-selected flow/offset values are still
required to ask the value-only hint kernel whether work is due. Nesting that
object under another wrapper would therefore preserve an oracle and could not
atomically settle A, B, Target, replay/ACK observations, terminal, or shutdown.

Physical R7 uses two endpoint roots. Each root is the sole composition point
for its endpoint-session clock and all actors belonging to that clock:

```text
OwnerPhysicalSessionRoot<T>
  phase / first root outcome / shutdown admission
  current A-or-B retained physical actor + active OwnerTarget handoff
  registered standby retained physical actor + exact OwnerStandbySlot
  optional retired actor during a handoff
  OwnerSessionController<T> + OwnerTargetExecutor<T>
  bounded ordinary output FIFO + independent byte inventory
  replay/application-ACK progress + internal due-work cursor
  pending probe response / probe timer / B freshness
  physical OwnerSwitchHint transaction
  exact status, terminal, legless-high-water and quarantine finalizers

ClientPhysicalSessionRoot
  existing Quinn client composition and retained A/B actors
  exact classified B control dispatcher
  physical hint validator
  one ClientSwitchArbiter for hard failure, local stall and authenticated hint
  one pending ATTACH epoch/nonce and shutdown admission
```

The old B-only owner is retained temporarily as an internal adapter while
ports are moved behind the full owner interface. It is not an acceptance root.
No final witness may be minted from it alone.

### Retained physical actor ownership

Every actor slot uses the already accepted R6 transition:

```text
Running(actor)
  -> Joining(join task that owns actor)
  -> Joined(actor witness)
  -> TaskLost(sticky cause)
```

Starting a join is synchronous and stores the unique task handle in the root
before any await. A cancelled waiter loses observation only. Actor-level join
failure returns the exact actor to `Running`; task loss is sticky and cannot be
reported as clean zero. Direct `join_for_shutdown(self)` ownership transfer is
forbidden.

### Owner output and due-work scheduling

One controller page becomes one root-owned ordered output FIFO. A page cannot
be followed by another page until the FIFO is empty, so its message bound is
the checked Target effect-step bound plus the separately owned continuation.
Every `Transmit(Frame)` is independently encoded once for inventory and is
then moved through the exact current actor FIFO; pressure restores the same
frame at the front. Lifecycle outputs are installed into typed root timer or
phase slots before the output is retired. `NeedsResume` never enters this FIFO:
the controller retains its non-cloneable continuation and the root schedules
only a value-only continuation category.

The root, not a harness, selects ACK-stall work. `OwnerReverseProgress` exposes
one bounded deterministic cursor over at most
`2 * (max_live_flows + max_terminal_tombstones)` retained directions. Live
capacity can be reused while terminal-grace progress remains, so the two
populations are independent and every addition/multiplication is checked.
The selector returns only a borrowed exact current entry and advances no state.
The physical hint transaction revalidates that entry, B freshness, and the
current episode under one synchronous root turn. Callers cannot supply a flow,
direction, oldest offset, ACK time, sequence, actor seal, or current time to
mint a hint.

### Control classification and status transaction

Controller/session pressure cannot head-of-line block leg control. Inside the
actor first-failure lock, a borrow-only encoded namespace/kind classifier makes
exactly one choice before policy preflight:

```text
session frame -> controller preflight; Blocked restores the same raw envelope
LEG_PROBE     -> exact responder; pressure retains one pending ACK token
LEG_PROBE_ACK -> exact B freshness transaction
SWITCH_HINT   -> exact physical validator/arbiter transaction
malformed     -> actor fatal
```

No decoded frame is dropped and re-encoded for retry. If borrow-only
classification cannot be proven, the actor instead retains one O(1)
`ClassifiedPendingControl` containing the original byte/arrival/ordinal
ownership.

An attach-generation status is settled in the same exact terminal/root
transaction. `Enqueue` may be abandoned only after endpoint/queue loss;
`Delivery` may be armed only by the exact delivered receipt, otherwise it is
abandoned on loss. Supervisor arm failure either returns the complete
publication unchanged before mutation or enters a sticky quarantine containing
it. No generic error may consume the publication.

### Fatal and switch arbitration

One root outcome arbiter records `ActorHardFailure`, `ControllerFailure`, or
`Shutdown` before physical cancellation. Controller failure must not call the
normal shutdown API and later appear as `LocalCancelled`. Shutdown first closes
new switch admission; a hard failure already committed by the actor retains its
hard cause. Within an open switch epoch, hard failure upgrades an existing soft
intent in place and never changes its epoch, nonce, or ATTACH.

### Exact ownership ledger

Each endpoint root reports one Authority-local checked ledger, not a collection
of object-local zero booleans. The owner root includes its actor channels/FIFOs,
controller and Target retained turns, replay/progress, probe/hint, joins,
endpoint, and session. The client root separately includes its A/B actors,
classified control, ordered delivery, validator, arbiter, pending ATTACH,
timers, endpoint, and session. Neither production root owns the other.

The no-oracle harness owns a third transit ledger for wire scheduler/events,
in-flight encoded records, Target/client-sink queues, and accepted bytes. It
computes a read-only checked aggregate from immutable endpoint snapshots; it is
not a production-global Authority and cannot mutate either endpoint. All three
ledgers retain independent summed/stored bytes. Terminal discard and shutdown
residual are distinct sticky inventories; historical discarded bytes are
evidence of explicit settlement and may not be counted as live zero or silently
overwritten.

This deepening is an implementation prerequisite, not a promotion. Physical
R7a-d remains open until both roots and the byte harness use these interfaces.

## 2026-08-31 controller/output authority convergence

Repeated review of the first physical wiring attempt found one invariant spread
across the actor, controller, output drain, rejection value, lifecycle caller,
and shutdown path. Further local patches are stopped until those authorities
converge behind one production boundary:

```text
OwnerPhysicalSessionRoot<T>
  closed physical topology and attach transaction
  OwnerControllerOutputRoot<T>
  probe / authenticated hint / switch arbiter
  root terminal transaction and whole-root ledger

OwnerControllerOutputRoot<T>
  controller port
  output page or drained continuation marker
  ordinary retry or rejected page
  lifecycle port -> root transit -> typed slots
  first terminal outcome, settlement audit, or sticky quarantine
```

The physical topology is a closed enum over current-only,
current-with-standby, current-with-retired, legless, and settling states. It is
not three independent optional actor slots. This keeps the active handoff with
the current actor, the authenticated standby slot/probe state with B, and the
total physical actor count at two or fewer. The production physical root is
constructed only by consuming the existing authenticated initial physical root
seed; the test-only B recovery composition is an adapter, not a production
root.

The controller/output root is likewise a closed state machine. Its relevant
states are live-vacant, live-page, live-drained, retry-deferred, in-flight,
rejected-page, terminalizing, stopped, and quarantined. A transaction guard is
installed in the root before entering controller mutation. A guard abandoned
before entry restores the exact input. Once controller entry starts, an
unknown unwind or forgotten guard remains visibly non-zero in-flight or sticky
quarantine; it may never normalize to a live or clean-zero state.

### Continuation capability and scheduling

The production continuation operation has no freely callable no-argument
entry point. A non-`Clone`, non-`Copy`, privately constructed per-turn permit is
minted only by the root-owned continuation transaction. That transaction calls
the controller and settles the closed outcome synchronously, with no `await`,
`?`, or fallible publication between mutation and settlement:

```text
Advanced
RetryableRetained
TerminalRetained
Aborted
Missing
```

Ownership disposition and scheduling severity are encoded in the enum rather
than in an independent boolean. A retryable retained continuation restores
exactly one marker and waits for a proven bounded external wake; it does not
self-spin and does not record controller failure. A terminal retained
continuation clears schedulability but leaves exact controller ownership in a
sticky terminal residual. Until a production wake/order seam is proved,
clock rollback, overflow, terminal phase, recovery overlap, wrong/lost Target
owner, missing/invalid resume, and orphan attach barrier are terminal retained.
Post-commit or invalid-page failures are aborted. Missing correspondence is
quarantine, not a reconstructed marker.

### Commit and publication rules

For ordinary input, continuation, output queueing, lifecycle installation, and
terminal discard, every validation, byte calculation, identity check, and
capacity check precedes the commit point. Final custody publication or actor
DELIVERY ACK is the last infallible step. In particular:

- ordinary controller mutation cannot precede installation of an in-flight
  root guard;
- output queue pressure retains the exact front record and its original byte
  ledger;
- rejected pages remain root-owned until exact retry, typed terminal discard,
  or sticky quarantine;
- a known aborted controller turn is a typed terminal residual that the root
  can audit and settle without ACK, not a generic permanent quarantine; and
- lifecycle ownership cannot escape to a caller-owned installed slot. The
  root exclusively owns `port -> transit -> typed destination`, including a
  generation-aware prepared replacement commit.

Attach validation is bidirectional. Only all-false and all-true are legal:

```text
physical slot present
  <=> transaction stage is Accepted
  <=> policy marker is AcceptedPendingCommit
  <=> actor-held acceptance is present
```

The all-true state additionally requires exact session, current/requested
generation, standby nonce, actor seal, FIFO/publication epoch, send ordinal,
accepted authority, and zero pending physical bytes. The validator runs only
at bounded construction, attach/switch, lifecycle, terminal, shutdown, and
no-oracle checkpoints, never on the DATA hot path.

### Cold inventory and capacity equations

Every retained frame exposes a validation-equivalent,
allocation-free `encoded_len_checked`. Cold inventories independently sum
those lengths with checked arithmetic and compare them with stored admission
bytes. Actor-arrival and local-origin provenance are counted as typed witnesses;
overflow, invalid encoding, identity mismatch, or an uncorrelated arrival is a
fail-closed audit error.

For checked limits `F = max_live_flows`, `Z = max_terminal_tombstones`,
`E = max_effect_steps_per_turn`, and
`D = MAX_ENCODED_DATA_FRAME_BYTES`, the root locks:

```text
reverse_progress_entries <= 2 * (F + Z)
scan_attempt_capacity = checked_add(completed_scan_episodes,
                                    progress_revision_resets_while_scanning)
scan_turns <= checked_mul(ceil(reverse_progress_entries / 32),
                          scan_attempt_capacity)
output_page_records <= E
output_page_bytes <= checked_mul(E, D)

controller.pending_continuations
  == live_page.continuation
   + live_drained.marker
   + rejected_page.continuation

live_page + rejected_page <= 1
ordinary_retry_inputs <= 1
all_inflight_transactions <= 1
physical_actors <= 2
probe_timer <= 1
probe_response <= 1
pending_hint + admitted_hint <= 1
lifecycle_port + lifecycle_transit <= 1
typed_lifecycle_actions <= Z + 3
```

All products and sums are checked. Copy-only bounded settlement history may
remain after stop, but no live message, byte, continuation, ACK, timer, actor,
endpoint, or session authority may hide inside that history.

The direction bound covers one stable progress revision. Every successful
progress mutation invalidating a live scan cursor consumes one separately
declared restart credit in the finite acceptance scenario. The scenario's
total scan bound is completed scans plus resets; an undeclared reset rejects
before mutation or remains a typed non-zero capacity failure. Production keeps
the fixed 32-entry per-turn bound and resumable cursor; it does not claim a
finite lifetime-turn bound under an indefinitely mutating session.

### Terminal settlement and branch order

Known fatal, cancellation, and shutdown paths use one root-owned no-ACK
transaction. It first receives and validates the committed-outcome token and
performs a read-only cold-ledger/capacity preflight over controller, output,
rejection, lifecycle, timer/probe/hint, actor queue, join, endpoint, and session
domains. One subsequent infallible root-state move seals admission, freezes the
first outcome and timer/probe/hint work, installs
`Terminalizing::Prepared`, and moves every actor into root-owned settling
custody. It then settles controller, output/marker, rejection, lifecycle,
hint/probe, and actor queues; synchronously installs every actor join task in
root state; and only then performs an await. Joined actors and remaining
terminal residuals settle before whole-root exact zero is validated and the
final infallible audit is published. Unknown panic or unrecoverable
interruption remains sticky non-zero.

Terminal preflight is read-only. Admission seal, first outcome, and
`Terminalizing::Prepared` custody commit in one infallible state move. A Drop
before that move leaves Live bit-identical; after that move but before
controller entry it retains the same staged non-zero transaction and never
restores Live. An unknown unwind after controller entry is quarantine.

Migration is by branch-by-abstraction: first seal actor/controller bypasses and
add allocation-free cold inventory; then move continuation and ordinary input
through the new root guards; then internalize rejection, known-abort,
lifecycle, and terminal settlement; then construct the production physical
root from the authenticated initial seed; only afterward wire physical B
probe/hint/arbiter and run the six-path no-oracle ledger. Each slice removes or
ratchets its old production path before the next is accepted.

This convergence remains an architecture prerequisite. It does not authorize
the public TUN path or any macOS TUN, VPS, WAN, throughput, or M2 run, and it is
not evidence that Task 4 or physical R7a-d is complete.

## 2026-09-01 physical-root cutover decision

The test-only B recovery composition has been moved into the private
`owner/physical_root.rs` adapter and its scalar-equal and pressure cleanup now
settles exact permits, arbiters, actor terminals, stamps, joins, and listener
outcomes. Physical attach `9/9`, legacy B `2/2`, terminal `5/5`, Knife16
provenance `158/158`, both code-growth gates, repository test provenance,
format, diff, and library check are green; independent re-review is Approve.
This closes the Move/fixture-cleanup stage only.

The architecture audit found two blockers before production behavior may
resume:

1. the no-oracle scan manifest still derives `2 * F`, but production correctly
   derives `2 * (F + Z)` because terminal-grace progress may coexist with a
   refilled live-flow population; the scenario must also charge every
   revision-invalidated cursor restart; and
2. controller/output exact-zero does not inspect `progress_entries`, so one
   retained progress record can be hidden by a false local zero.

The only safe production construction seam consumes
`QuinnOwnerInitialPhysicalRootSeed<T>` or the paired one-shot
`QuinnOwnerInitialAuthenticatedStandby<T>` into a new private
`OwnerPhysicalSessionRoot<T>`. That root owns current A, its active handoff, the
sole controller/executor/session chain, B candidate/standby, progress,
probe/hint/arbiter, joins, terminal state, endpoint/session Authority, and one
whole-root ledger. The `#[cfg(test)]` B-only adapter remains gated.

The root Authority must exist before B admission mutates a FIFO. The
authenticated-standby migration constructor performs only read-only checks and
publishes a stable candidate-holding root state. That root then installs an
in-root `InstallingCandidate` guard before `OwnerStandbySlot::admit`; unknown
unwind stays visibly non-zero in the root, while successful admission ends in
one infallible topology move. “Publish the root after admit” is forbidden.

The existing admission routine is not yet the commit primitive because it
allocates hint-registration state after queue push. The production B slice must
precompute encoding, validation, all allocation/cloning, byte/capacity checks,
and prepared slot state while the candidate remains root-held. Queue admission
is the last fallible operation; after its receipt, only infallible moves into
the root topology are allowed. Pressure restores the same prepared transaction.

The current controller/output hard-terminal success is exact preservation but
not exact zero: it retains a discarded terminal, actor stamp, and controller
legless high-water. Production migration therefore has two phases inside one
root-owned terminal transaction:

```text
controller/output terminal apply
  -> exact non-zero {discarded terminal, stamp, high-water}
  -> root-owned actor joins and stamp-pair validation
  -> consuming controller/output finalizer
  -> ControllerOutputStoppedZero
  -> whole-root ledger zero
  -> final infallible Joined audit publication
```

Unknown unwind after controller entry remains in-flight or sticky non-zero
quarantine. Fatal, cancellation, and shutdown share this no-ACK settlement;
only their already-committed first-outcome token differs.

The exact failure tree, ledger, code-growth-aware implementation sequence,
allowed first-slice files, and stop rules are frozen in the 2026-09-01 result.
The first behavior slice is the capacity correction, and it requires a fresh
user confirmation after independent architecture review.

## Non-goals

- no public `client_tun::run_event_loop` selection change;
- no UDP, owner server binary, second public ingress, R8, or R9;
- no D16, Endpoint, MTU, pool, congestion-control, queue, replay, or timeout
  tuning;
- no macOS TUN, VPS, WAN, throughput, M2, or M3 run; and
- no harness callback that reads fault state or directly switches to B.

## Current reachability and exact defect

The current inbound path is:

```text
Quinn RecvStream
  -> quinn_leg_actor::data_reader_task
  -> complete header/body read
  -> InboundPublicationTracker::reserve(publication_epoch)
  -> InboundRecord
  -> bounded mpsc
  -> ControllerInboundRecord
  -> StartedQuinnLegActor::try_transact_*inbound
  -> QuinnClientSessionComposition::try_receive_current
  -> ClientSessionController::receive_current_from_quinn
  -> SessionSupervisor::apply_current_event_observed
```

`publication_epoch` correctly closes the queue-Empty/in-flight-publication
race, but it is ordering evidence only. `quinn_client_root::RootClock` is
currently created after the initial actor and composition. The root later
passes its category-service `now` into composition/controller. Scheduler delay
therefore changes ACK deadline evidence.

The `SWITCH_HINT` codec, `HintSequence`, and closed
`ReverseApplicationAckStall` cause already exist. No production owner producer,
client consumer, rejection matrix, or shared trigger arbiter currently exists.

## P0: endpoint-session monotonic clock and arrival envelope

### Authority

Create one cloneable, opaque `SessionMonotonicClock` backed by one fixed Tokio
`Instant` epoch and a source identity. It is created before the endpoint's
first physical actor and shared by that endpoint's A actor, B actor,
composition/controller, and root. Client and owner endpoints use their own
session clocks; every actor within one endpoint session must match its root.

The clock exposes only:

- checked `now() -> Result<SimTime, SessionClockError>`;
- checked observation for deterministic tests; and
- `same_source(&Self) -> bool`.

Backwards time and nanosecond overflow fail closed. There is no fallback to
root dequeue time or `SimTime::ZERO`.

The initial branch-by-abstraction path may let initial actor construction mint
the endpoint clock immediately before the actor starts, then let composition
adopt that exact source and require replacement actors to match it. The final
production constructor must make the pre-actor source explicit. A/B may never
mint independent epochs.

### Atomic capture

After a complete DATA body has been read, one
`InboundPublicationTracker::reserve_arrival` critical section performs:

```text
checked session-clock capture
  -> reserve next publication epoch
  -> mark exact in-flight epoch
  -> return { arrived_at, publication_epoch }
```

The capture and publication reservation share the existing reservation lock.
This prevents a timer from observing neither a queued record nor an in-flight
record after bytes have already arrived. Header-only input is not admitted.

Both values stay in the actor-owned envelope through:

- `InboundRecord`;
- `ControllerInboundRecord` and its blocked pending slot;
- the typed `QuinnLegActorInbound` path;
- controller and receive-before-timer callbacks; and
- composition/controller classification.

Blocked/retry paths move the same timestamp; they never recapture it.
`EncodedDelivery` remains transport-independent and does not carry local clock
metadata.

The root's current time remains scheduling authority for due timers and new
deadlines. `arrived_at` is evidence for probe/application ACK deadlines and the
first positive B ACK. Publication epochs remain the ordering/high-water
authority and are not removed.

### Terminal capture

An exact transport failure time is captured in the same session clock when
`ActorFirstOutcome::Open` first becomes `Failed`, not after child cleanup or
joined terminal publication. Later exact-terminal handoff carries that fixed
time. A probe deadline retains its absolute timer instant; local shutdown stays
a separate typed cause.

### Capacity

The code-level reachability review replaces the earlier logical
`8 * (inbound_messages + 1)` estimate with the checked peak-storage contract
below. `SimTime` is eight bytes, but the actor can simultaneously own the
bounded channel values and four fixed arrival words: the controller pending
slot, the synchronous callback envelope, the producer reservation, and the
atomic in-flight-arrival witness.

```text
arrival bytes = size_of::<SimTime>() * (live.inbound_messages + 4)
terminal bytes = size_of::<Option<SimTime>>()
terminal witness handles = 2 * size_of::<ActorTerminalStampWitness>()
actor timestamp bytes = checked sum of the three terms
```

The arrival value moves with each record; the three additional fixed copies
are synchronization/callback witnesses and do not allocate another queue entry
or payload. The terminal instant has one canonical `Option<SimTime>` inside
`ActorFirstOutcome`. Exactly one private `ActorTerminalStampPair` mint creates
the handoff and root-outcome handles needed during cleanup overlap; the witness
type is non-`Clone`, and later layers can only move either handle. Join proves
both handles name the same actor, discards one, and moves the survivor through
joined/composition state. Constructor arithmetic uses `size_of` and checked
addition/multiplication, and rejects overflow before actor publication.

## P1: reducer-owned application-ACK observation

The supervisor remains the replay mutation owner. It exposes opaque bounded
observations only after the matching reducer receipt commits:

- replay became outstanding for exact
  `(session, generation, flow, direction, start, end)`;
- a positive application ACK advanced an exact offset and released nonzero
  bytes; and
- the flow/direction became terminal.

Decoded ACK, QUIC ACK, `RECORD_DELIVERED`, duplicate/equal/non-advancing ACK,
zero release, and reducer rejection do not start or refresh the progress clock.

The outer role controller combines those reducer facts with the actor's
immutable `arrived_at`. Progress state is bounded to at most two directional
entries per configured live flow or retained terminal tombstone. Admission
above the checked `2 * (max_live_flows + max_terminal_tombstones)` bound fails
closed; no ACK history is retained.

The two endpoints do not possess symmetric knowledge. The owner reducer is the
sole authority that can prove that reverse replay is outstanding and name its
exact oldest offset. In an ACK-only blackout the client's accepted frontier
may be ahead of the owner's peer-ACK frontier; when the first reverse DATA
segment is lost the client cannot even prove that owner replay exists. No
client-side check may pretend to reconstruct either owner fact.

For each live directional entry retain only:

```text
oldest outstanding offset
latest positive application-ACK offset/time
generation and terminal bit
```

Replacement recovery closes only on the first positive B application ACK that
intersects the exact replay range. Queue insertion, byte delivery, and physical
delivery ACK cannot close the recovery interval.

## P2: authenticated owner hint

One owner-side session controller, composed with `OwnerTargetExecutor`, owns
the reducer observations, registered physical B slot, B probe sequence and
freshness, hint sequence, aggregate ACK-stall timer, and pending hint. The
harness may drive that owner but may not assemble those authorities or mint a
hint itself. The root allocates one additional checked aggregate timer slot;
it must be production-dispatched rather than merely occupied in a test.

The owner controller may mint one `SWITCH_HINT` only when all conditions hold:

- the supervisor has Target-to-client replay outstanding for the exact live
  flow/direction and oldest offset;
- its last positive application ACK is old enough under an epoch-bound stall
  timer;
- the exact registered B slot is live and has a fresh matching probe ACK;
- session, current generation, standby nonce, B seal/binding, negotiated
  `STANDBY_CONTROL_V1`, and hint sequence match; and
- no same-epoch hint is already pending or admitted.

The fixed leg-control path implements the real responder for an incoming
`LEG_PROBE` on both endpoint roles. Owner-side B health becomes fresh only
after a matching B `LEG_PROBE_ACK` received through the registered actor; a
test-injected ACK or manually advanced freshness flag is not admissible
evidence.

The hint is queued through B's existing bounded leg-control queue. Queue
pressure returns the exact pending hint and sequence authority. It grants no
DATA, ACK, Target, replay-release, terminal, or generation capability.

Owner hint state is O(1): one next sequence, one pending hint, and one last
published sequence. Per-flow ACK state remains bounded by P1.

## P3: client validation and switch arbitration

The client accepts a hint only as a classified exact-B `LegControlFrame` with
the actor arrival timestamp. Validation requires:

- exact B process-local seal and live registered-standby ownership;
- exact session, current generation, standby nonce, negotiated feature, and
  closed cause;
- `Direction::TargetToClient`;
- an exact live, non-terminal flow whose locally accepted frontier is at or
  beyond the hinted offset;
- fresh B health at the record's `arrived_at`; and
- strictly increasing `HintSequence`.

Wrong seal/session/generation/nonce/sequence/direction/cause/offset, an idle or
terminal flow, stale B, or a duplicate/lower hint returns a typed rejection
whose before/after snapshot is identical. Rejection cannot consume a nonce,
start ATTACH, advance generation, or refresh B health.

Here “wrong offset” is deliberately limited to facts the client can know:
zero/invalid range, unknown or terminal flow, wrong direction/generation, or
an offset ahead of the local accepted frontier. A hinted offset lower than the
local accepted frontier is valid during ACK loss. The owner mint gate, tested
separately, rejects idle/no-replay state and any offset other than the
reducer-owned exact oldest replay offset.

Hard failure, local client-to-Target stall, and accepted owner hint enter one
`ClientSwitchArbiter`. The first valid trigger for the current epoch reserves:

```text
one switch epoch -> one attach nonce -> one exact-next ATTACH transaction
```

All concurrent/late triggers observe the same pending transaction or return
snapshot-inert stale/duplicate results. Only correlated `ATTACH_ACCEPTED`
commits the next generation. Failed/retryable queue admission retains the same
nonce and exact standby owner; it never mints a second transaction.

If a soft hint/local-stall trigger reserved the transaction and A terminal is
then observed while the same B ATTACH is pending, the arbiter upgrades that
same pending nonce/request to post-loss semantics. It consumes the exact A
terminal but does not create a second attach. The correlated response chooses
normal versus post-loss installation from that upgraded state; the old split
`begin_*standby_registration` entry points are adapters into this one pending
transaction, not parallel ownership paths.

The existing hard-failure/probe paths remain active but must enter this arbiter
before attach construction. No parallel old attach path may remain reachable.

## P4: deterministic no-oracle harness

The memory harness may schedule bytes, physical close, timer expiry, queue
pressure, and Target behavior. It may not inspect a fault script from a
controller, fabricate reducer events/effects, or call a raw switch function.

Required tracer bullets:

1. Arrival prerequisite: complete inbound at `deadline - 1ns`, `deadline`, and
   `deadline + 1ns`, delay root service past the deadline, and prove exact
   accept/accept/reject behavior.
2. Capture/publication race: while publication is blocked, a due timer returns
   its exact `PublicationPending` token; after release, before/equal input wins
   and after input remains stale without extending the absolute deadline.
3. Block/retry: advance the clock between retries and prove timestamp, payload,
   ordinal, permits, and queue ownership are unchanged.
4. R7a: reverse-only A DATA/ACK blackout with healthy A probe and healthy B,
   but no authenticated hint, produces zero attach/generation change.
5. R7b: exact owner replay plus ACK stall emits one hint on B; the client
   starts one attach; replay is byte exact; first positive B ACK occurs inside
   the checked combined horizon.
6. R7c: one case for every wrong seal/session/generation/nonce/sequence/
   direction/cause/offset, idle flow, stale B, duplicate, and lower sequence;
   each proves identical snapshots and zero attach.
7. R7d: simultaneous hard failure, local stall, and hint under all deterministic
   orderings consume one epoch/nonce and commit at most one generation.
8. Success, rejection, pressure, cancellation, fatal, and shutdown end with
   exact zero queue, byte, ACK, replay, timer, actor, endpoint, and session
   ownership.
9. Repeat the arrival/timer and three-trigger races `100` times.

All new test bodies live in private `src/<module>/tests/` child files or root
`tests/` public-contract/provenance targets. Production files contain only
external test Module seams.

## Implementation order

1. **Complete:** RED/GREEN the endpoint-session clock and atomic arrival
   envelope without changing switch behavior.
2. **Complete:** route actor `arrived_at` through composition/controller and
   remove root observation time as inbound deadline evidence.
3. **Partial:** the positive-ACK observation seam and bounded policy storage are
   complete; production reducer-derived outstanding/terminal export and its
   owner-controller consumer remain open.
4. **Next, confirmation required:** correct the necessary-only manifest to
   consume typed `OwnerProgressCapacity` and prove checked `F + Z`, direction,
   scan-turn, completed-scan plus revision-reset bounds, and overflow. This
   slice does not touch controller/output.
5. In a separate controller/output slice, RED/GREEN the retained
   `progress_entries` false-zero predicate after the required test-Module
   shrink.
6. Behavior-preservingly move the complete hard-terminal Implementation out of
   the shrink-only `continuation.rs`; only a later green-to-green slice removes
   test gating and changes ordering/state.
7. RED/GREEN the production controller/output exact-preservation transaction,
   then construct `OwnerPhysicalSessionRoot<T>` from the exact initial seed and
   authenticated B transaction. The B-only adapter remains test-only.
8. RED/GREEN the whole-root hard-terminal exact-zero finalizer, bidirectional
   attach transaction, reducer observation consumption, B probe/freshness, and
   authenticated hint production in separate coherent slices.
9. Route physical hint rejection and hard/local/hint triggers through the
   accepted single arbiter, including same-pending hard upgrade.
10. Adapt the deterministic byte harness for R7a-d with no fault oracle and prove
   success, rejection, pressure, fatal, cancellation, and shutdown settlements.
11. Repeat full focused/repeat, broad local, provenance, governance, release,
    vendored, format/diff, and independent review gates after production
    composition is reachable.

## Stop rules

Stop R7 acceptance if any of these is required:

- root dequeue time, publication epoch, QUIC ACK, or physical DELIVERED is used
  as application arrival/acceptance evidence;
- A and B have independent clock sources within one endpoint session;
- a retry recaptures time or allocates a new switch nonce;
- the harness directly switches based on fault knowledge;
- an unauthenticated/wrong/stale hint changes a snapshot;
- two triggers create two epochs, nonces, ATTACH transactions, or generations;
- application progress or hint history is unbounded;
- either checked combined path exceeds the retained horizon; or
- a closed R1-R6 ownership/lifecycle invariant regresses.

An expected R7 RED authorizes only its matching minimum implementation. An
unexpected regression requires a causal analysis and revised modification plan
before another behavior edit.

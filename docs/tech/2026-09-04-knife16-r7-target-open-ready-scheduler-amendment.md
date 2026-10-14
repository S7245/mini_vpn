# Knife16 R7 Target-open and ready-scheduler progress amendment

Date: 2026-09-04

Last revised: 2026-09-05.

Status: **P1 ARCHITECTURE ACCEPTED / IMPLEMENTATION NOT ACCEPTED**

Heisenberg closed all six original findings and returned P0/P1/P2 `0/0/0`,
Approve, on the 666-line architecture candidate, SHA-256
`fffa018e408490c80503a184b0cbf02bc7eb088f7abb2fd07e132b928740b4d6`.
Reviewer identity: `01a06bfa-4552-7ea3-b5ce-9d6b65f88992`. Historical Darwin
was unavailable; this is explicitly a replacement independent review, reused
through repair closure. Architecture/Clean Architecture/System Design are
`10/10` only within this amendment, not runtime evidence or percent complete.

The accepted [Case C architecture](2026-09-04-knife16-r7-case-c-owner-server-root-architecture.md)
owns process lifetime, whole-root custody, adoption and final settlement. The
[implementation plan](2026-09-04-knife16-r7-case-c-owner-server-root-implementation-plan.md)
owns `P1 -> P2T -> P2A -> P2B -> P3` and later gates. This amendment resolves
only the asynchronous TCP Target-open and ready-scheduler design questions.
Architecture acceptance itself authorizes no Rust or external acceptance.
The user's separate goal to proceed until entering P2T authorizes its bounded
test/provenance Move entry after status review; it does not authorize P2A,
P2B or P3 behavior. Case C, R7, Task 4/6 and public TUN remain open.

## 1. Decision and evidence

Extend the existing crate-private, statically dispatched `TargetIo` with one
concrete, move-only `OpenProgress`, begin/poll/finish and a borrowed audit. Each
`OwnerTargetExecutor` flow owns its exact request, source, open state, socket,
result and reservations. The process scheduler owns ready leases and cursors;
its index contains no command, socket, task result or whole session payload.

Select a named, directly borrow-polled DNS/TCP state machine. There is no
per-open spawn, worker pool, task handoff or system-resolver task. Four process
classes remain: `Settlement`, `Bootstrap`, `TargetOpen`, `Session`. Each
aggregate class has durable local readiness for independently waking sources.
Shutdown changes scheduling routes through one root-owned migration.

Source facts:

- [target.rs](../../src/owned_upstream/target.rs) has synchronous `open` and
  terminal-only `TargetOpenCompletion`; MemoryTarget completes immediately.
- [owner_target.rs](../../src/owned_upstream/owner_target.rs) immediately maps
  open to `PeerOpenResolved`. Its `PendingTargetCompletion::Open` already
  retains reducer-blocked completion and remains the post-open continuation.
- [session.rs](../../src/owned_upstream/session.rs) has two bounded lanes;
  `try_send_*` exact-returns on Full/Closed; awaited send owns the command.
- No production owner-server root or mailbox owner exists. Actor
  capture/scan/wait is a protocol shape, not a process scheduling Authority.
- Local Cargo.lock selects Tokio 1.52.0. Its `net/addr.rs` string resolution
  calls `spawn_blocking`; wrapping `lookup_host` would retain the spawn gap.
  TCP exposes `poll_write_ready`, `take_error` and `from_std`; UDP exposes
  borrowed `poll_send_to`/`poll_recv_from`. socket2 exposes borrowed
  nonblocking connect. These support named progress without opaque futures.

These are source-level feasibility observations, not executable P3 evidence.
DATA stays on the per-session actor/controller/Target path. No global DATA
queue, application-global lock, per-record allocation or full-table scan is
introduced. DNS transport here is internal TCP-open resolution, not VPN UDP
delivery; that product Interface keeps its separate architecture gate.

## 2. Alternatives and placement

The original four parallel Interface explorations remain useful:

| Alternative | Decision |
|---|---|
| Extend TargetIo, per-open task and global completion route | Retain the existing port; remove task/result handoff entirely. |
| Separate sealed TargetOpenAdapter | Reject a second generic provider/socket-transfer Interface without a second policy consumer. |
| Enqueue/drive/observe owning operation handles | Retain borrowed observation; extra caller lifecycle handles are unnecessary. |
| Process ready class for each task/send/open source | Use four policy classes with bounded local source indexes. |

```text
owner/server_root.rs                   process lifecycle and fixed slots
  server_root/scheduler.rs             ready leases, cursors, budgets, park
  server_root/tcp_target.rs            concrete TCP/DNS progress Adapter
  session slot -> whole physical root -> controller/output root
    -> OwnerTargetExecutor<T: TargetIo> -> exact per-flow OpenProgress
```

Policy depends on the inward Target port; MemoryTarget and TCP implement it.
`Context`, `Poll` and `Waker` may cross the private poll seam. Tokio/socket/DNS
types stay in the Adapter; Main and protocol effects see none of them.
Deleting server_root removes process lifecycle and its caller route; deleting
the scheduler removes its fairness Implementation; deleting a Target Adapter
leaves policy and the other Adapter intact. No re-export/getter/callback seam
is added for progress internals; no second lifecycle owner is introduced.

## 3. Split-phase port and exact errors

Illustrative signatures; a later symbol-first slice must compile the concrete
shape without `dyn`, boxed dynamic futures or a second provider:

```rust
trait TargetIo {
    type OpenProgress; // named, move-only, redacted
    fn begin_open(&mut self, key: TargetOpenAttemptKey, target: &TargetAddr)
        -> Result<Self::OpenProgress, TargetOpenBeginError>;
    fn poll_open(&mut self, progress: &mut Self::OpenProgress,
        intent: TargetOpenIntent, cx: &mut Context<'_>)
        -> Poll<Result<(), TargetIoError>>;
    fn finish_open(&mut self, progress: Self::OpenProgress)
        -> TargetOpenDisposition;
    fn open_inventory(&self, progress: &Self::OpenProgress)
        -> Result<TargetOpenColdInventory, TargetOpenInventoryError>;
    // Existing byte I/O, terminal, readiness, inventory and join operations.
}
```

`begin_open` constructs cold state after root reservations; it starts no DNS,
socket, timer registration or task. It does not retain the temporary Target
borrow: its bounded owned representation is included in request capacity.
Pending leaves exact progress in the executor. Ready(Ok) means preparation or
cancellation settlement is complete. Ready(Err) transfers the exact error to
the owner, which stores it together with still-owned progress in quarantine
before returning observation. The port does not also claim to own that same
error. No Debug/Clone/Copy requirement is added to it.

`open_inventory` is the sole added cold-audit operation on this existing inward
port, not a provider/getter exposing Adapter fields. The concrete Adapter can
read its named progress while the generic executor only borrows it. The method
scans actual progress-owned socket/registration fields, DNS buffers/addresses,
timers, result/fault storage and any unexpected task/handle. It returns one
fixed-size value-only `TargetOpenColdInventory`: phase/correlation, physical
counts, independently summed bytes, stored mirrors and unresolved dispositions.
It neither returns Authority nor polls, allocates, closes, registers or moves
resources. All progress phases, including Cold and Quarantined, are auditable.

At an exclusively borrowed cold boundary, the executor calls this operation
for every retained progress and checked-sums those records, then combines them
with `TargetIo::cold_inventory()` for Adapter-owned live/terminal state. The
two scans cover disjoint owners: an installed socket is counted by the live
scan, not again by its CompletionHeld record. Outer flow/open/ready/command
leases are scanned by their owners and bidirectionally checked against the
combined physical inventory and Section 9 envelope. Physical presence, stored
mirrors and reserved/unresolved leases remain distinct columns; one cannot
stand in for another. Any scan error/mismatch retains the exact owner and
blocks zero. No default-zero record, downcast or Adapter-specific executor is
allowed. This is cold control work, never a full scan on each DATA record.
The existing live capacity/inventory value types must gain their missing
physical socket/resource fields in the same future cutover; their current
MemoryTarget-only counters are not already sufficient TCP audit evidence.

`finish_open` is callable only with prepared success or settled failure under
the owner lifecycle commit exclusion. It moves an already configured socket
to a reserved live position, or consumes socket-free settled failure. It
performs no I/O, registration, validation, allocation, cancellation or await.
MemoryTarget follows the same phases. The synchronous `open` route is deleted
at cutover; `TargetOpenCompletion` stays terminal/value-only, with no Pending.

## 4. Direct-poll feasibility and spawn exclusion

The selected Adapter uses named variants containing concrete nonblocking
sockets, fixed buffers, cursors, exact outcomes and pinned concrete timers.
It needs no stored anonymous async function type. Pinning a concrete timer at
reserved construction is allowed; dynamic future erasure is not. No open
operation calls `spawn`, `spawn_blocking`, string `ToSocketAddrs`, libc blocking
resolution or a resolver that starts internal jobs. Its task/handle count is
exactly zero in every phase. This removes the pre-handle spawn-hook transaction
from this route; a closed start gate and unknown marker did not close it.

Numeric Targets proceed to connect. Domain Targets use a private bounded
wire-DNS state machine driven through borrowed socket readiness:

- Config supplies a checked finite list of numeric recursive resolvers;
  no resolver address needs DNS or runtime discovery. Bind validates config
  before accepting work. Domain support is mandatory.
- A/AAAA, CNAME depth, retries, parser steps and retained addresses have finite
  configuration/protocol bounds. Query ID, endpoint and full question correlate
  every reply. Unmatched replies consume a bounded work unit.
- DNS UDP and TCP fallback are successive phases with one resolver socket at
  a time. Truncated UDP replies enter bounded TCP framing; exhausted policy
  yields flow-local failure, never unbounded answers or blocking fallback.
- One query buffer, one response buffer and address array are reserved first.
  Oversize responses fail within those bounds. DNS retries use actual protocol
  deadlines; they are not scheduler retry polling.

The first Adapter has one connecting Target socket per attempt, trying the
bounded address list serially. Concurrent address racing is a later gate.
Nonblocking socket connect, borrowed write readiness, `take_error` and
peer/correlation checks precede PreparedSuccess. Socket options are set
before connect or after completion, respecting platform restrictions.

Runtime I/O/timer availability is preflighted. Every consuming registration or
close primitive needs an audited exact disposition: returned socket, or proved
local-handle release with its outcome retained. Tokio `from_std` consumes its
input on error; it cannot be labelled exact-returning. The Adapter accounts
explicitly for that consumed-close path. Unknown disposition retains the real
surviving state and an unreleased lease, forbids retry and zero. An error code
alone is not release evidence.

P3 must verify primitive semantics against supported platforms and selected
dependency versions before Adapter implementation. A background resolver or
worker, unbounded parsing, blocking close or unaccountable registration changes
this design and stops that slice. No worker-start fallback or process
fail-stop exception is introduced by this amendment.

## 5. Owning phases and finalization

```text
AttemptKey = root incarnation + slot generation + SessionId
           + SessionFlowId + exact request correlation + open generation
PendingTargetOpen owns request + Target + source + all reservations
  Reserved -> Cold -> Resolving -> Connecting -> PreparedSuccess
  cancellation before commit -> CancelSettlement / LateSocketSettlement
                             -> SettledFailure
  PreparedSuccess | SettledFailure -> CompletionHeld -> Live | OpenRejected
  precommit fault -> Quarantined { exact stage, first fault, leases }
```

Cancellation is sticky intent on the same progress. If cancellation wins
after connect becomes ready but before commit, PreparedSuccess changes to
LateSocketSettlement. `poll_open` drives all required shutdown/close, resolver
release and timer cancellation; it does not call `finish_open` yet. Only a
proved socket-free settled outcome becomes SettledFailure. Failed cleanup
retains the exact residual/error in quarantine and blocks completion.

Local close means release of the owned descriptor and reactor registration,
not remote ACK, peer shutdown or absence of kernel TIME_WAIT. An explicit
consuming release primitive may mechanically use a destructor after its release
contract is audited; arbitrary observer/root Drop is never settlement.
Platform-unknown close disposition never refunds a lease or retries a
potentially reused descriptor. All asynchronous preparation stays in pollable
settlement phases before finalization.

Final cancellation validation and PreparedSuccess installation share the
existing per-owner lifecycle exclusion. It is bounded, with no user callback
or fallible operation. Cancellation winning returns to settlement; install
winning makes subsequent cancellation the live Target flow's responsibility.
This is one linearization edge, not a new global lock. No socket, result or
unpublished completion escapes between preparation and owning publication.

## 6. Open transaction order

1. Validate identity, generations, complete request, phase and duplicated state.
2. Reserve flow/open/ready/resolver/socket/timer/result/command storage from one
   checked plan. Pressure retains/returns exact request/source and every acquired
   lease before side effects.
3. Consume LocalFlow through its existing source factory; retain derived source
   and Target. Publish Reserved before Adapter begin.
4. Store cold progress; publish this attempt's local ready bit first, then its
   session's TargetOpen aggregate cell.
5. One bounded visit selects a ready attempt, polls by borrow and advances its
   persistent cursor. Pending retains state and its generation-bound Waker.
6. Ready publishes PreparedSuccess/SettledFailure root-first. Revalidate under
   lifecycle exclusion; cancellation may require further pollable settlement.
7. Once commit is permitted, `finish_open` moves to the reserved live slot or
   socket-free failure. Store the exact PeerOpenResolved in the reserved
   `PendingTargetCompletion::Open` in the same synchronous owner transaction.
8. Reducer pressure retains that continuation; it does not reinstall a socket
   or rebuild a command. Reducer application/publication is final.
9. Return each lease only at its domain's proved release/transfer edge.

A slow open leaves the front of pending_effects and releases the sole
OwnerTargetResume. Other flows remain serviceable. Duplicate/late outcomes
are correlated with owning phase, never adopted by copied ID. Stale
socket/result custody is quarantined; late Wakers are notifications only.

## 7. Failure, observation and isolation

Refusal, bounded DNS failure, timeout, policy rejection and modeled resource
pressure are flow-local. Impossible phase/correlation, invariant error or
unknown socket disposition is session-local fatal; exact survivors and first
fault remain sticky nonzero while other sessions continue.

Ready-lease corruption, root wake-source corruption, generation/revision
exhaustion and physical-versus-stored scheduler inventory mismatch are
root-fatal. Admission closes and only proven settlement may run. They do not
mint zero or normalize to retry. Ultimate root destruction remains governed
by Case C's process custodian, not a new Drop implementation here.

Observer cancellation drops only a borrow and temporary wait registration.
Progress, results, source readiness and commands remain in their owners.
Flow reset/expiry/shutdown changes sticky intent on that same state.

## 8. Send-side command custody

Each session reserves one cell for Control and one for OrderedSource. A
nonempty cell stops another pull on that lane. Each inbox lane has a separate
bounded prefetch position; prefetch cannot become a hidden third queue.

```text
Vacant -> Held { exact command, lane, origin, lease }
       -> TransferringUnknown { correlation, lane, retained lease }
          -> mailbox-owned                      on Ok
          -> Held { exact returned command }    on Full
          -> ClosedReturned { exact command }   on Closed
          -> SendQuarantined { exact survivors, unknown disposition, fault }
```

Store Held before observer yield. Publish the send-source bit, then its
aggregate. Transfer only through nonblocking `try_send_*`. On Full retain the
command, arm lane-capacity notification and recheck its capacity epoch before
parking. The sole receiver publishes capacity progress after dequeue; close
publishes terminal progress. A raced epoch keeps the send bit; unchanged Full
clears it until actual capacity/close progress. Full is not continuously
runnable and does not retry on every session visit.

Closed retains the exact command for expected shutdown settlement or live
closure quarantine. Unknown send never retries and never presents correlation
as a missing command. This is the accepted plan's sticky-unknown boundary,
not an exact-return claim. Production calls no `send(command).await`.

## 9. Construction capacities

All symbols are checked typed config or existing capacity witnesses. Let
`N=C_session>0`, `F>0`, `0<O<=F`, four process classes and one root-only
Settlement cell. Uniform per-session bounds may use the maximum admitted
witness; heterogeneous witnesses cannot exceed this envelope.

`A>0` bounds retained addresses; `Q_dns/W_dns>0` bound full query/answer
buffers including TCP framing; `D_addr/D_request` bound address/request
storage. `U_resolver>0` bounds numeric resolver config. CNAME/retry/parser
limits are checked positive finite counts; buffer lengths fit protocol fields
and allocations. Each attempt reserves one deadline and one protocol retry
timer, reused across substages.

`M_ctl/M_data>0` bound inbox lanes. Existing typed witnesses supply per-session
reducer-held `R_cmd`, effect `E_cmd` and non-open completion `X_cmd` positions.
`D_cmd` bounds all owned bytes of any admitted command/effect, including payload,
Target and variable fields. A local flow/source capability remains additionally
in its existing domain; bytes cannot substitute for it. Physical-root capacity
supplies `L_actor>0` actor/endpoint settlement positions. Root capacity supplies
`L_root>0` listener/endpoint continuations, not a guessed constant.

```text
C_open          = N * O
C_ready         = N * 4 + 1
C_attempt_ready = C_open
C_target_ready  = N * F
C_actor_ready   = N * L_actor
C_source_ready  = N * (5 + 10) + L_root
C_task          = C_handle = C_worker = 0          // Target-open only
C_result        = C_open                         // terminal result/attempt
C_fault         = C_open * 2                     // open + cleanup faults
C_timer         = C_open * 2
C_dns_query     = C_dns_answer = C_open
C_dns_address   = C_open * A
C_dns_bytes     = C_open * (Q_dns + W_dns + A * D_addr)
C_resolver_cfg  = U_resolver * size_of_numeric_endpoint
C_request_bytes = C_open * D_request
C_open_socket   = C_open                         // serial Target connects
C_dns_socket    = C_open                         // UDP OR TCP resolver socket
C_live_socket   = N * F
C_socket_total  = C_open_socket + C_dns_socket + C_live_socket
C_send          = N * 2
C_prefetch      = N * 2
C_mailbox       = N * (M_ctl + M_data)
C_reducer       = N * R_cmd
C_effect        = N * E_cmd
C_completion    = N * X_cmd + C_open
C_command       = C_send + C_prefetch + C_mailbox
                  + C_reducer + C_effect + C_completion
C_command_bytes = C_command * D_cmd
```

All additions/products, storage sizing and generation bounds are checked before
bind/admission; overflow rejects config, never saturates. A moved command is
charged to one owner; reserved destination space is not another live command.
Physical bytes are independently summed and checked against stored mirrors.
One open retains at most one terminal result and two exact faults; sequential
intermediate failures are consumed before another address attempt, not retained
in an unbounded error list.

Connecting, prepared, resolver, late, live and quarantined sockets all consume
leases in C_socket_total. Install transfers to its pre-reserved live domain;
the source lease is not freed before that move. Old live sockets may coexist
with open/resolver sockets only within the total envelope. DNS UDP is released
before TCP fallback takes its one resolver-socket position.

Every domain conserves `Free + Reserved + sum(owned phases) = Capacity`.
Open phases are Reserved/Cold/Resolving/Connecting/CancelSettlement/
LateSocketSettlement/PreparedSuccess/SettledFailure/CompletionHeld/Quarantined.
Send phases are Held/TransferringUnknown/ClosedReturned/SendQuarantined.
Results/timers/buffers/readiness are phase fields, not free-floating counters.
Pending/live flows also conserve the shared F flow-position witness; O is a
sublimit, not permission for F+O live flows. Z continues to bound only accepted
session SessionFlowId terminal tombstones.

## 10. Ready ownership at both levels

Process cells are keyed by `(slot, slot generation, scheduling generation,
class)`; attempt/Target/actor cells add stable subslot and generation.
Session has five source bits: ControlSend, OrderedSend, ControlInbox,
OrderedInbox and OwnerContinuation. Inner owner iteration keeps its existing
bounded fairness; there is no drain-until-empty loop.

```text
Dormant -> Idle -> Published -> RunningClean -> RunningDirty
                                 \              /
                                  -> Clearing -> Idle | Published | Quarantined
```

The cell owns its lease. Publishers/Wakers are non-owning correlation. Commit
work to its owner, publish its local bit/revision with Release ordering, then
the aggregate summary and root epoch. Duplicates coalesce. A wake while
running/clearing dirties the same lease with newer revision. Both levels use
preallocated summary words. Waker clones allocate no source/task/lease. Stale
generations cannot schedule a reused occupant; wrap/exhaustion fails closed.

For O=2, A's wake marks A before the aggregate. If a visit polls B, A's bit
remains set. The aggregate cannot clear until its local ready index is empty.
Pending alone is not readiness; every pending attempt is not busy-polled.
Cold progress and runnable bounded parsing steps keep their own bits until
genuinely waiting on registered readiness.

## 11. Clear, recheck and root park

1. Validate generations, enter RunningClean and advance the process cursor
   before work. Select a local ready source using its persistent cursor.
2. Perform one bounded unit: TargetOpen polls one ready attempt; other attempts
   keep their bits. Target/actor lists use the same selection rule.
3. Runnable/dirty work republishes behind the cursor. Otherwise enter Clearing,
   capture revision, clear its index bit and Acquire-recheck its source.
4. Inbox recheck can dequeue only into reserved prefetch. Held send uses its
   armed capacity/close epoch. Pending open uses its installed Waker and local
   dirty revision; no second poll occurs outside budget.
5. CAS to Idle only with unchanged revision and no source work. Concurrent wake
   invalidates that CAS; otherwise restore Published. Summary clearing requires
   child-word recheck and restoration when publication raced.
6. The aggregate clears only after the same protocol confirms its entire local
   summary empty. Every local publish republishes/dirties the aggregate.

Wake before clear dirties the lease, during clear changes revision, and after
Idle publishes afresh. Publication completes only after bit-to-summary-to-epoch
ordering. Root park captures wake epoch, arms its waiter, rescans summaries,
rereads epoch and awaits only empty+unchanged. Observer re-entry starts with
that scan. Outer `select!` never selects fairness order.

## 12. Four budgets and executable bounds

```text
B_settlement > 0; B_bootstrap > 0; B_open > 0; B_live > 0
B_total = B_settlement + B_bootstrap + B_open + B_live
B_session = 1 bounded unit per selected process-cell visit
C_settlement = N + 1; C_bootstrap = C_open_class = C_live = N
```

Each turn visits classes in that order with dedicated slices and no borrowing.
Persistent circular selection puts serviced cells behind the cursor; no second
visit bypasses another ready cell's opportunity. Continuous readiness, not
network latency, is the premise.

For positive c,b use checked `c/b + (c%b != 0)` for ceil division, never
overflowing `(c+b-1)/b`. N=0 is rejected. Empty actual ready sets invoke no
division; C in the following formula is the positive capacity.

```text
T_cell(c) = 1 + ceil(C_c / min(B_c, C_c))
Q_cell(c) = T_cell(c) * B_total
T_open_attempt <= O * T_cell(TargetOpen)
T_session_source <= 5 * T_cell(Session)
```

The leading 1 covers publication after this class's slice in the current turn.
All budgets, sums, ceil results and all bound products are checked at config
time; overflow rejects the plan. Bounds cover service delay, not remote I/O,
protocol completion, throughput or zero. Each parser/copy/cleanup unit has a
finite byte/item cap; poll returns after it and self-publishes if useful work
remains. A source waiting only on I/O retains no ready claim.

## 13. Shutdown class migration

Shutdown closes admission first. Each occupied slot retains its original
owning phase in `Migrating { custody: MigrationCustody, old registrations,
new scheduling generation, rebind cursor }`. MigrationCustody is a closed,
nonrecursive, non-Clone/non-Copy enum of the accepted slot payloads below;
it contains no Migrating/Vacant variant or optional owning hole. The move keeps
all original reservations, dispositions and faults exactly once. It neither
constructs a physical root for pre-auth work nor rebuilds an earlier phase.
Slot generation is unchanged; scheduling generation advances once. A slot
already routed to Settlement resumes its current stage without remigration.
Vacant slots need no session cell; pending listener accept stays root-owned.

| Original owning variant retained inside MigrationCustody | Settlement route and explicit first service |
|---|---|
| PreAuthReserved | Bootstrap: retain the exact handoff/bootstrap plus leases; first service records cancel and enters its existing owned disposition path. |
| Bootstrapping | Bootstrap: borrow the same task/result custody, request cancel, install the new Waker and observe/join that same outcome; no second start. |
| AuthenticationPending / AuthenticationRejected | Bootstrap: retain outcome, candidate or rejection settlement and leases; first service records rejection/shutdown, never repeats authentication or publishes an accepted session. |
| BootstrapQuarantined | Bootstrap: preserve first fault and exact surviving custody; service only proven settlement; unknown work stays nonzero, never retries. |
| PreparingSeed | Migration: retain the exact seed; first settlement service uses its existing consuming `into_parts()` and publishes PreparingParts with the same leases under the owning guard. No standalone physical-root construction. |
| PreparingParts | ActorEndpoint and ControllerOutput, plus any actual owned Target/open/command sources: keep the exact actor, handoff, controller and reservations; first service marks shutdown and rebinds their existing progress. No Live publication after shutdown wins. |
| Live / Terminalizing / Quarantined | Preserve the whole root and original live/terminal/fault subtype; reroute its actual sources and poll each once with sticky shutdown; no seed reconstruction or join restart. |
| Reclaimable | Migration: retain SessionJoinedZero and parent leases; first service resumes bounded parent-slot retirement, without polling a nonexistent actor or minting another zero capability. |

Enum alternatives retain the distinct original payloads even where a table row
shares a route. A phase-derived source mask enables only sources actually owned
by that variant. PreparingSeed-to-Parts and subsequent settlement keep the same
migration progress and registered route; they do not restart its generation.
Sources revealed by seed decomposition enter the residual rebind set before
that transition is published, even if their index is behind the cursor. Each
source is visited once; migration cannot finish with an unvisited revealed
source. The one seed decomposition step uses the existing `+1` bound below.
Terminal dispositions and required downstream cleanup remain governed by the
accepted Case C contract; an unavailable settlement operation leaves that exact
phase nonzero. This scheduling amendment does not invent their implementation.

In one bounded, no-await transaction under slot lifecycle exclusion:

1. Validate the original custody variant; preflight generation increment and
   the reserved Settlement registration. A failed preflight retains that phase.
2. Publish Migrating; activate its Settlement cell with Migration ready.
3. Invalidate only publishers belonging to that variant; retire their process
   claims and return their leases. No nonexistent live registration is minted.
4. Publish Settlement summary/root wake before any observer sees the result.

Each class pool separately conserves `free + reserved + active = capacity`.
Up to three live claims are returned, not converted into one settlement lease.
The migration stage owns old claims until their returns are recorded, plus
the new one once activated. No source is charged to two work budgets.
Unexpected unwind retains that exact stage and all unreturned leases in
quarantine; no vacant slot or clean observation escapes.

Rebinding the variant's actual sources is incremental. Migration's cursor
visits each eligible source once, sets sticky cancel and marks it ready for
its first settlement poll. Later migration visits cannot reset that source.
Poll installs its new Waker and rechecks real progress. Old wakes may be stale,
but the explicit first poll prevents loss. An unvisited source stays owned
and indexed by that cursor. Migration finishes only after all sources have
been visited, without a table scan in one work unit or a polling timer.

## 14. Session-local settlement fairness

A Settlement cell has ten fixed categories with persistent ready mask/cursor:
Migration, Bootstrap, Open, ControlSend, OrderedSend, ControlInbox,
OrderedInbox, ActorEndpoint, Target, ControllerOutput. Ineligible categories
are empty. Each visit selects one ready category and one bounded child unit;
every category has quantum 1. Commands follow their exact shutdown disposition,
never silent discard. Failed owners retain faults and unresolved inventory.

Open reuses its O attempt cells, Target its F flow cells and ActorEndpoint its
L_actor cells; they are rerouted, not copied to another ready owner. Other
categories have one bounded continuation. Migration visits at most
`M_migration = O + F + L_actor + 5 + 1` sources. Local cells share the
generation/dirty/clear/recheck protocol; I/O Pending clears only its own claim.

```text
K_set = 10
M_max = max(1, O, F, L_actor, M_migration)
T_settlement_item <= K_set * M_max * T_cell(Settlement)
Q_settlement_item <= T_settlement_item * B_total
T_root_item <= L_root * T_cell(Settlement)
```

All are checked conservative bounds, including category and child competition.
Continuously ready Migration gets service to finish its bounded rebind pass
without starving other categories. Dependency-waiting work is republished by
its actual completion/epoch source. The root-only Settlement cell round-robins
its L_root listener/endpoint continuations. This supplies service, not an
invented whole-root finalizer: R7 no-ACK/terminal/root-ledger gates still precede
SessionJoinedZero and OwnerServerJoinedZero.

## 15. Inventory and false zero

Cold inventory scans physical fields and independently checks stored mirrors
for every Section 9 capacity; process/local leases and summaries; migration;
command bytes; results/faults; resolver buffers/sockets/timers; and
live/late/quarantined Target handles through the borrowed open/live audit split
in Section 3. Migration scans its exact variant's payload and parent leases,
including pre-auth custody and seed/parts without assuming a physical root.
Unknown release is an unresolved lease,
never falsely reported as a scanned handle. Discovering an open task
contradicts the zero-task design.

No slot reuse or slot-generation advance occurs with nonzero inventory.
Scheduling-generation advance is the separately proven owning migration; it
does not authorize slot reuse. Free backing memory is not live ownership.
Z, controller/executor/handoff/output/actor/endpoint/session domains remain
separate; actor zero is insufficient for session/server zero.

## 16. Repair findings and acceptance counterexamples

| Issue | Revised contract | Required counterexample test |
|---|---|---|
| P1 late socket at finish | Pollable cancel/late settlement, fused prepared commit | Cancel after connect Ready, before install; retain socket until settled. |
| P1 aggregate lost wake | Attempt readiness before aggregate | A wakes, B selected/Pending; A remains ready without second wake. |
| P1 incomplete capacity | Checked maxima, borrowed progress scan plus disjoint live scan | Retain resolver socket but zero its mirror: generic audit must reject; live plus progress totals do not double-count. |
| P1 spawn-hook handle loss | No tasks/workers/implicit resolver spawn | All numeric/domain/cancel paths keep task/handle inventory zero. |
| P1 shutdown starvation | Exact original-phase migration and ten fair categories | Bootstrap Pending and PreparingSeed/Parts shutdown retain original custody; revealed sources rebind; busy work cannot starve other categories. |
| P2 zero/overflow bound | Positive N, safe ceil, checked products | N=0, sum/product overflow, exact maximum, arbitrary-turn publication. |

Other required branches: pre-side-effect pressure exact-return; begin/poll
unwind with retained survivors/fault; observer cancel and repeated Ready
without duplicate effects; Full until real capacity progress; exact Closed;
unknown send without retry; stale generations; both-level clear/recheck races;
flow-local dial failure; corrupt scheduler without false zero; DATA isolation.

## 17. Conditional implementation and test order

P2T -> P2A -> P2B still precedes P3 after P1 review and separate authorization.
P1 creates no Rust route or scaffold. P3 uses one behavior and one causal
RED/minimum GREEN at a time:

1. One compiling root/scheduler route with behavior, no empty child.
2. Command staging and exact Full/Closed/unknown dispositions.
3. Process/local readiness, clear/recheck and service bounds.
4. Characterization/Move as needed before replacing synchronous open;
   MemoryTarget exercises the same Pending/Ready/cancel/fatal port.
5. Platform/DNS primitive audit, then direct TCP/DNS Adapter and separate
   cancellation/pressure/late-socket tracers through that port.

Keep the six previously reserved exact filters:

- `pending_ready_command_observer_cancellation_retains_exact_command`;
- `ready_command_full_and_closed_return_the_exact_command`;
- `target_open_pending_retains_exact_authority_until_completion`;
- `cancelled_target_open_observer_reenters_the_same_completion`;
- `ready_session_enqueued_during_cursor_handoff_is_not_lost`;
- `busy_session_cannot_starve_another_ready_session_past_the_derived_bound`.

Also preserve these additional exact filters:

- `target_open_start_pressure_returns_exact_work_without_side_effect`;
- `target_open_wake_during_poll_requeues_the_same_attempt_once`;
- `target_open_cancel_race_settles_late_socket_before_resolution`;
- `stale_ready_generation_cannot_schedule_a_reused_session_slot`;
- `ready_clear_recheck_closes_every_publication_interleaving`;
- `ready_class_budgets_cannot_be_stolen_by_an_earlier_class`;
- `open_send_ready_inventory_blocks_false_session_zero`.

The two remaining review closures additionally require:

- `target_open_cold_inventory_rejects_retained_resolver_with_zero_mirror`;
- `bootstrap_pending_shutdown_migrates_the_same_custody`;
- `preparing_seed_shutdown_rebinds_sources_revealed_by_decomposition`;
- `preparing_parts_shutdown_retains_parts_without_live_publication`.

Add one distinct tracer per Section 16 counterexample. Use deterministic turns
and protocol deadlines, not sleep-based evidence. Compile RED must name the
predicted missing seam.

Each coherent behavior requires appropriate focused tests, race-justified
repeats, direct regressions, provenance, fmt/diff/check, both governance gates
and independent review. Unexpected RED/Request Changes stops before another
repair. Oversized files stay net-zero/shrinking; characterize and Move a deep
slice first when needed. Only the Integrator changes debt inventory.

## 18. Stop rules and review boundary

Stop on a new provider/dynamic future/callback/public Interface; task/worker/
system-resolver handoff; blocking owner I/O; socket lost without proved
release; fallible work after finish; unbounded parsing/buffers; missing local
readiness; full-table hot scan; sleep/select order as fairness; overflow
fallback; source/slot reuse with custody; copied token as physical Authority;
arbitrary Drop as settlement; or source-size/visibility/warning-allowance
growth outside accepted governance.

Mechanical checks and source observations are not runtime correctness evidence.
The same replacement Reviewer approved the repaired architecture with no
remaining findings. This promotion changes status/evidence only; Sections
1–18 retain the reviewed design. Implementation readiness remains the accepted
4/10 baseline. Current status, handoff, TODO and reusable learning memory are
synchronized at this promotion boundary. P2T must still freeze exact bodies,
routes and baseline evidence before its first Move; no runtime claim follows.

## 19. Source links

- [Current status](../current/STATUS.md)
- [Product rules](../../Rules.md)
- [Domain language](../../CONTEXT.md)
- [ADR-0015](../adr/0015-path-diverse-resumable-upstream.md)
- [Case C architecture](2026-09-04-knife16-r7-case-c-owner-server-root-architecture.md)
- [Case C implementation plan](2026-09-04-knife16-r7-case-c-owner-server-root-implementation-plan.md)
- [Target port](../../src/owned_upstream/target.rs)
- [Owner Target executor](../../src/owned_upstream/owner_target.rs)
- [Owner mailbox](../../src/owned_upstream/session.rs)
- [Quinn actor](../../src/owned_upstream/quinn_leg_actor.rs)
- [Quinn composition](../../src/owned_upstream/quinn_leg_composition.rs)

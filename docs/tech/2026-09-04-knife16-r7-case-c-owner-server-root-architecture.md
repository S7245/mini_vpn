# Knife16 R7 Case C / Task 6 owner-server-root architecture

Date: 2026-09-04

Status: **ACCEPTED / ARCHITECTURE-ONLY / RUST NOT AUTHORIZED**

Independent architecture review closed at P0/P1/P2 `0/0/0`, Approve, after
closing the original five P1 and two P2 findings. Architecture-only correctness
is `10/10`; implementation readiness remains `4/10`.

This sidecar selects the missing process-lifetime ownership role for R7 Case C.
Its acceptance covers only that architecture boundary. It does not accept an
implementation, close Case C, authorize a production cutover, or accept R7a-d,
Task 6, the public TUN path, or an external gate.

## 1. Decision in one sentence

The existing domain role **Upstream session owner** is selected as the future
concrete deep Module `OwnerServerRoot`: a process-level root that privately owns
bounded pre-auth ingress/bootstrap custody, authenticated session custody,
complete per-session physical roots, Target-resource construction, and shutdown
Authority, while a future `mini_vpn-upstreamd` Main remains only the outer
composition Adapter.

`OwnerServerRoot` is an architecture-selected future symbol. It does not exist
in the current tree. The decision here is its Authority, lifetime, dependency
direction, and caller contract. The exact Rust Module route and minimum
visibility remain a separate pre-implementation privacy gate because the
current physical root is private inside
`quinn_leg_composition::owner::physical_session_root` and the current binary
cannot name the crate-private owned-upstream implementation.

## 2. Goal

Choose the one production parent that can close Case C without splitting the
per-session settlement Authority:

- it outlives every `OwnerPhysicalSessionRoot<T>` it admits;
- it owns each whole physical root from publication through terminalization;
- it keeps exact actor, FIFO, replay, ACK, timer, controller/output, Target,
  endpoint, and session ownership reachable during controlled shutdown;
- it permits observer cancellation without abandoning Authority;
- it converts to clean joined state only after a whole-server exact-zero proof;
  and
- it keeps the DATA hot path local to each session rather than routing packets
  through a global custody table or lock.

## 3. Non-goals

This accepted architecture does not:

- implement `OwnerServerRoot`, `mini_vpn-upstreamd`, or a caller route;
- accept the existing CurrentOnly prototype for production exposure;
- absorb the initial-A legacy Authority;
- connect production terminal preservation or the no-ACK finalizer;
- create the real TCP Target socket Adapter or design the separate UDP inward
  Interface and Adapter;
- connect authenticated B, physical switching, or runtime restart credit;
- close whole-root or whole-server exact zero;
- change the public TUN path from `LegacyOwnedUpstream`;
- promise process-crash persistence or cross-process Target socket migration;
- choose a speculative provider trait, dynamic dispatch boundary, or global
  registry; or
- authorize Rust, tests, Cargo, VPS, WAN, throughput, or M2 work.

## 4. Source-grounded current facts

### 4.1 Domain and process boundary

- [`CONTEXT.md`](../../CONTEXT.md) defines **Upstream session owner** as the
  one server-side process/lifecycle owner of Target sockets and authoritative
  resumable state. `OwnerServerRoot` is the concrete Module name for that
  existing role, not a new domain concept.
- [ADR-0015](../adr/0015-path-diverse-resumable-upstream.md) selects one stable
  owner behind independent ingress paths. It explicitly excludes owner-process
  crash persistence and Target-socket migration between owner processes from
  the first-stage contract.
- [`src/main.rs`](../../src/main.rs) has client and probe modes only. There is no
  `mini_vpn-upstreamd` composition root or server run mode.
- [`src/lib.rs`](../../src/lib.rs) keeps `owned_upstream` crate-private and
  dormant for Task 4/6. Therefore the future binary-to-root route is absent.

### 4.2 Current per-session physical capability

- [`owner_initial.rs`](../../src/owned_upstream/quinn_leg_composition/owner_initial.rs)
  defines `QuinnOwnerInitialPhysicalRootSeed<T>` with private fields and the
  reversible consuming `into_parts` / `from_parts` seam. Its current accepted
  alternatives include authenticated-standby, terminal-tracked, and initial
  composition exits.
- [`physical_session_root.rs`](../../src/owned_upstream/quinn_leg_composition/owner/physical_session_root.rs)
  defines the private `OwnerPhysicalSessionRoot<T>` and the private
  `from_initial_seed` constructor. Its only caller is a test; there is no
  non-test construction, storage, service, or destruction path.
- The accepted test-reachable constructor reversibly decomposes the seed,
  performs its modeled preflight, calls
  `QuinnOwnerControllerOutputRoot::new(controller)` as its final fallible
  operation, exact-reconstructs the seed on failure, and builds CurrentOnly on
  success. That is evidence for the inner build sequence only. Production must
  absorb its success arm into the parent slot's fused `PreparingParts -> Live`
  transition; returning a standalone `Ok(root)` for later insertion is forbidden.
- The accepted direct-join slice keeps the same actor in the root and closes
  living-root observer-cancellation, pre-await unwind, and exact modeled actor
  error retention. It does not close ultimate destruction of the root.
- The exact evidence and limits are recorded in the
  [direct-join local result](2026-09-03-knife16-r7-current-only-direct-join-local-results.md)
  under the accepted
  [direct-join amendment](2026-09-03-knife16-r7-current-only-direct-join-amendment.md).

### 4.3 Existing primitives are not the missing parent

- [`QuinnLegActorListener`](../../src/owned_upstream/quinn_leg_actor.rs) owns a
  Quinn endpoint, driver, actor-capacity registry, and listener shutdown state.
  Its registry counts live and terminating actors; it is not an authenticated
  `SessionId`-to-owner custody table and cannot drain session/Target Authority.
- `QuinnLegActorListener::admit` awaits the concrete transport accept and yields
  either `RetrySent` or a pre-auth `QuinnLegActorBootstrap`. That result is an
  ingress/bootstrap event, not an authenticated session port or attach commit.
- `QuinnLegActorBootstrap::start` currently moves bootstrap work into a spawned
  task. Dropping an unclaimed `ServerBootstrapHandoff` starts detached cleanup
  and does not return its exact verdict to a process root. That path is
  fail-closed locally, but it is not the required root-retained bootstrap/task/
  result custody seam.
- `StartedQuinnLegActor::join(&mut self)` can prove actor-local terminal/FIFO
  settlement. Actor `Drop` cancels and aborts its nursery; that is not settlement.
  Listener `Drop` closes the endpoint; that is not whole-server settlement.
- [`SessionOwnerMailbox`](../../src/owned_upstream/session.rs) provides bounded
  control/data lanes and bounded control-burst fairness. It has no production
  owner loop, and its cloneable `SessionOwnerIngress` is a mailbox sender—not
  authentication or session Authority. Its async send path also has no current
  whole-server transaction proving exact command return when the sending
  observation is cancelled.
- [`SessionSupervisor`](../../src/owned_upstream/supervisor.rs) is the sole
  mutable owner of the pure reducer and its adapter-side permits/authorities,
  including replay, attach/active/catch-up, factory admission, uplink, and
  quarantine ownership. [`OwnerTargetExecutor<T>`](../../src/owned_upstream/owner_target.rs)
  owns that supervisor, the Target, accepted-session flows, the
  `SessionFlowId`-keyed flow-terminal tombstones, and bounded pending work.
  Neither is a process lifecycle root.
- [`TargetIo`](../../src/owned_upstream/target.rs) is the existing inward
  Interface for TCP/byte-stream Target flows. `MemoryTarget` is its bounded
  deterministic Task 4 test Adapter. The tree has no production TCP socket
  Adapter/factory, and it has no UDP packet/datagram inward Interface.
- [`QuinnClientRoot`](../../src/owned_upstream/quinn_client_root.rs) is a client
  session root and has the wrong dependency and lifetime direction.
- [`LegacyOwnedUpstream`](../../src/owned_upstream/legacy.rs) owns no background
  lifecycle, while [`client_tun.rs`](../../src/client_tun.rs) still selects it
  for the public path.
- The cfg(test) two-leg harness and B-only physical adapter are evidence tools,
  not production owners.

These facts rule out reusing an existing object as the Case C custodian.

## 5. Chosen Module and Interface

### 5.1 Module depth

`OwnerServerRoot` is one deep process-level Module. Its Interface hides:

- Quinn listener and bootstrap details;
- pre-auth bootstrap custody, authenticated session lookup, and slot phases;
- `QuinnOwnerInitialPhysicalRootSeed<T>`;
- `OwnerPhysicalSessionRoot<T>`;
- actor and join state;
- controller/output and Target executor internals;
- replay, FIFO, ACK, timer, and shutdown ledgers; and
- concrete TCP socket Adapter implementation and, after its own architecture
  gate, the future UDP ownership/Adapter implementation.

Deleting this Module must delete the production owner-server lifecycle. No
caller may retain a second protocol for joining, terminalizing, or settling a
session.

### 5.2 Caller-facing lifecycle Interface

The caller-facing Interface has at most these three lifecycle entries. Names are
frozen here for discussion; exact Rust signatures await the route/privacy gate.

```text
bind(config, concrete_adapters) -> Result<OwnerServerRoot, ExactStartFailure>

drive_until(&mut self, shutdown) -> BorrowedOrValueOnlyObservation

try_into_joined(self) -> OwnerServerJoinedZero
                       | NotJoined { exact_whole_server_root }
```

Contract:

1. `bind` (or a mechanically equivalent `start`) owns all resources it creates.
   A modeled failure returns or exactly settles every acquired resource; it
   cannot publish a partially owned server.
2. `drive_until` borrows the root. Dropping the observer ends only the borrow;
   the same root remains authoritative and the drive is re-enterable. Its
   observation contains no owning capability.
3. `try_into_joined` consumes the root. It yields `OwnerServerJoinedZero` only
   after the whole-server ledger is exactly zero; otherwise `NotJoined` returns
   the exact whole root, not an error category or copied inventory.

The Interface must not expose `submit_leg`, a physical root, seed, actor,
`JoinHandle`, custody getter, or second join operation. Ingress admission remains
a private Adapter-driven operation inside `drive_until`.

### 5.3 Rust route/privacy gate

The current tree cannot simultaneously satisfy all three claims:

1. the server root is lexically outside per-session roots;
2. the current physical root remains fully child-private; and
3. no Module move or visibility change is needed.

Before Rust begins, a separate symbol-first architecture slice must select one
concrete Module route and prove:

- the future binary can name only the opaque `OwnerServerRoot` facade;
- the server implementation can consume and drive the whole physical root;
- physical root, seed, actor, and custody types do not escape through the
  caller Interface;
- any effective visibility change is the narrowest possible library-internal
  scope, never a re-export or public physical-root surface; and
- no pass-through, getter, callback, parameter, macro, `include!`, or dynamic
  dispatch diffusion is introduced to overcome privacy.

This gate is a prerequisite to an implementation plan. This document does not
invent a filesystem path or line-count budget before that proof.

## 6. Required non-test caller chain

The future production reachability chain is:

```text
mini_vpn-upstreamd Main                 outer composition Adapter
  -> OwnerServerRoot                   process lifecycle Authority
     -> Quinn ingress event            Humble transport/ingress Adapter
        -> bounded pre-auth owning slot
           -> root-retained bootstrap/task/result custody
              -> authentication + attach policy
                 -> Target/controller/accepted initial seed
                    -> fused CurrentOnly construction
                       -> whole-root slot publication
```

This chain is a requirement, not a description of current code. No link in the
current non-test tree stores an `OwnerPhysicalSessionRoot<T>`.

Main may parse configuration, install signals, construct concrete Adapters, and
call the three lifecycle entries. It must not drive per-session joins, hold
physical roots, inspect custody, or decide exact-zero settlement.

## 7. Ownership graph and Authority XOR

```text
OwnerServerRoot
├── bounded Quinn ingress resources
│   ├── listener / endpoint / endpoint driver
│   └── ingress/bootstrap event seam
├── bounded owner-session custody table
│   └── each slot owns its exact phase payload
│       ├── pre-auth handoff/bootstrap/task/result custody, or
│       └── exactly one complete OwnerPhysicalSessionRoot<T>
│           ├── current actor and actor-contained FIFO
│           ├── initial handoff / identity correlation
│           ├── controller and output root
│           │   └── OwnerTargetExecutor<T>
│           │       ├── SessionSupervisor / reducer-side permits
│           │       ├── accepted-session flow-terminal tombstones (Z)
│           │       └── TargetIo (TCP)
│           ├── replay / ACK / timer / progress state
│           └── typed restart-credit state
├── concrete TCP Target socket Adapter factory
├── future UDP Target custody behind a separately accepted inward Interface
├── future parent identity retention, only behind a separate bounded gate
├── bounded control/lifecycle scheduler and ledger
└── shutdown Authority
```

The existing R7 `Z` population is owned inside an accepted session root and is
keyed by `SessionFlowId`. The optional parent identity-retention line is only a
future obligation if the protocol proves it needs late or duplicate pre-auth or
session-identity retention: its exact name and quantitative bound are not
selected here, and it cannot consume or alias `Z`.

Listener routes and directory/index entries are lookup Adapters. They may point
to a slot identity but never own, clone, or lease session Authority.

After publication, exactly one parent slot owns the whole physical root. Every
service, direct-join, terminal, no-ACK, or settlement operation borrows that
slot-held value. No per-session spawned task may own the whole root.

The forbidden substitutes are:

- extracting only actor custody while leaving controller/Target/handoff behind;
- `Arc`, cloneable tokens, or copied inventories presented as ownership;
- a global registry or janitor with competing lifecycle responsibility;
- a detached or abort-on-Drop cleanup task;
- a test fixture, B-only adapter, client root, or legacy wrapper; and
- an `Ok(root)` returned to a caller for later fallible insertion.

## 8. Owning-slot state machine

Every bounded table entry is always in one explicit phase:

```text
Vacant
  -> PreAuthReserved {
       exact listener handoff/bootstrap capability,
       exact session-slot and destination leases
     }
  -> Bootstrapping {
       exact bootstrap task/result custody,
       the same leases
     }
  -> AuthenticationPending {
       exact bootstrap outcome and authentication candidate,
       the same leases
     }
     +-> AuthenticationRejected {
     |     exact rejection settlement,
     |     the same leases
     |   } -> Vacant
     +-> PreparingSeed { exact accepted seed, all reserved destinations }
          -> PreparingParts {
               exact actor, handoff, and controller,
               all reserved destinations
             }
          -> Live { exact whole physical root }
          -> Terminalizing {
               exact whole physical root,
               first terminal disposition
             }
          -> Reclaimable {
               SessionJoinedZero,
               remaining parent runtime leases
             }
          -> Vacant
```

An unknown bootstrap disposition for which the root still owns the exact
bootstrap/task/result capability enters a distinct pre-auth state:

```text
BootstrapQuarantined {
    exact bootstrap/task/result custody,
    all still-owned leases,
    first fault,
    sticky nonzero inventory
}
```

For every potentially unwind-capable postcommit borrowed turn, the parent first
keeps the whole root in its slot and publishes the applicable sticky/in-progress
disposition. If an outer catch boundary preserves the parent root, it records
the first fault and consuming-transitions that same slot to:

```text
Quarantined {
    exact whole physical root,
    all still-owned reservations,
    first fault,
    sticky nonzero inventory
}
```

Properties:

- `Vacant` is a stable available-table phase, not a transient `Option::None`
  hole during an ownership transition.
- While `QuinnLegActorListener::admit` is pending on transport accept, the parent
  still owns the borrowed listener and no admitted bootstrap exists. Once it
  yields a bootstrap, `PreAuthReserved` must own that exact capability before
  any subsequent await, observer return, or cancellation point.
- `Bootstrapping` owns the exact task/result capability. Dropping an observer
  ends only the borrow; re-entry continues the same outcome. Its internal
  move-only phase owns either the in-flight task or the completed exact result,
  never both and never neither.
- Authentication and attach commitment are `OwnerServerRoot` policy. The Quinn
  listener only reports transport/bootstrap events.
- `AuthenticationRejected` must exact-settle its bootstrap result before a
  consuming transition releases the same leases and returns the slot to
  `Vacant`. It neither owns nor consumes `Z`. If the protocol later requires
  late or duplicate pre-auth/session-identity retention, a separate architecture
  gate must freeze an independently bounded parent capacity domain before such
  retention exists; this state machine does not invent that domain.
- `PreparingSeed` owns the exact seed; a copied identity or inventory is not a
  seed. `PreparingParts` owns the exact result of consuming `seed.into_parts()`.
- `Live` and `Terminalizing` own the entire physical root, never only a handle.
- `Reclaimable` owns the whole-session-zero capability and remaining parent
  runtime leases until a consuming reclaim releases them and returns the slot
  to `Vacant`. It owns no `Z` tombstone or `Z` lease.
- `SessionJoinedZero` is the exact capability held by `Reclaimable` after
  per-session-root settlement, including exact zero of every accepted-session
  flow-terminal `Z` tombstone and lease. It does not zero the still-owned parent
  slot, route, scheduler, directory, or other runtime leases.
  `OwnerServerJoinedZero` is not a session-slot phase; it exists only as the
  final whole-server conversion result.
- A future pre-auth/session-identity tombstone, if required, belongs to a
  separately designed bounded parent domain. It is never an accepted-session
  flow tombstone, never consumes `Z`, and is not claimed to exist today.
- `BootstrapQuarantined`, `Reclaimable`, and `Quarantined` are nonzero until
  their exact payload and leases are consumed by the specified settlement.
- Postcommit `Quarantined` holds the same whole physical root and first fault.
  It never rebuilds a seed, reconstructs Running, or reports clean zero.
- If unwind escapes the parent-preserving catch and destroys the process root,
  no exact quarantine claim is made; that is the fail-stop boundary in
  Section 14.
- Phase transitions consume the previous owning state. There is no duplicate
  owner and no category-only replacement for an owning payload.

Exact seed return is promised only for modeled `Result`/pressure branches.
This architecture does not claim that an arbitrary panic while exact parts are
temporarily owned on a precommit stack can return the seed. If caught precommit
unwind is to become supported, a future implementation must first prove a
phase-tagged guard that owns the exact actor/handoff/controller and reservations,
then lock it with a deterministic failpoint. Until that proof, any precommit
unwind gap is a stop and cannot report clean zero.

The current `QuinnLegActorBootstrap` spawned-task and unclaimed-handoff Drop
cleanup do not satisfy these pre-auth states. A separately accepted seam must
return or retain the exact bootstrap task/result ownership in `OwnerServerRoot`
before the first await or cancellation point after that ownership is created;
detached cleanup cannot be promoted as evidence.

For CurrentOnly phases inside a live physical root, the parent always owns the
same whole root while its private actor-owning state advances through
`Running`, `Joining`, `Joined`, or `Failed`. Moving between those states never
moves the physical root out of the parent slot.

## 9. Fused adoption transaction

This transaction starts only after pre-auth bootstrap and authentication have
produced `PreparingSeed`. A later physical-build rejection returns the exact
seed to the same parent-owned slot; it does not rewind authentication into an
unauthenticated connection. Earlier pre-auth/pre-seed failures settle or return
their own exact aggregate, not a seed that does not yet exist.

The fused `PreparingParts -> Live` slot transition is the commit point. The
transaction order is:

1. Before taking a seed, complete the admission gate and every check that uses
   only parent configuration or concrete Adapter state: duplicate route,
   available slot, directory, scheduler/ledger, and reservable destination
   capacity. Reserve their exact typed leases.
2. The parent-owned transaction takes the exact accepted seed. No caller keeps
   another seed or ownership token.
3. Immediately consume `seed.into_parts()`—the only real inspection seam—to
   obtain the exact actor, handoff, and controller. No getter or wider Interface
   is introduced.
4. Perform seed-specific correlation, identity, clock, runtime, FIFO,
   acceptance, capacity, inventory, Target metadata, and destination preflight
   on those exact parts. Finish every remaining fallible parent operation while
   the transaction owns the parts and all leases.
5. Every modeled `Result` or pressure error before commit uses the real
   `QuinnOwnerInitialPhysicalRootSeed::from_parts(actor, handoff, controller)`
   seam to reassemble and exact-return the same seed, then releases or returns
   the same parent reservations. Clone, copied inventory, and scalar
   reconstruction are forbidden.
6. Call `QuinnOwnerControllerOutputRoot::new(controller)` as the final fallible
   physical-root build operation. Its error returns the same controller; the
   transaction reassembles the same actor, handoff, and controller into the
   exact seed.
7. The successful `new` arm synchronously and infallibly constructs CurrentOnly
   and consuming-transitions the already prepared parent slot directly to
   `Live { exact whole physical root }`. Successful output-root construction,
   CurrentOnly construction, whole-root move, and slot publication are one
   fused commit edge—not separate operations, a later publication call, or a
   nameable unpublished `Ok(root)`.

Inside that fused successful arm there is no intervening:

- `await` or observer boundary;
- allocation or collection growth;
- callback or logging side effect;
- validation, socket operation, channel operation, or other `Result`;
- spawn, handoff to another task, or cancellation point; or
- externally observable intermediate root.

After success, the seed no longer exists and the same whole root is already in
the `Live` slot. Before a later potentially unwind-capable borrowed turn, the
root must publish the relevant sticky/in-progress disposition while keeping the
whole root in the slot. A catch boundary outside that borrow may then record the
first fault and move the same root to `Quarantined`. There is no after-the-fact
ownership reconstruction. If unwind escapes and destroys `OwnerServerRoot`, the
process boundary is fail-stop; it is not a successful quarantine transition.

The exact-return guarantee above covers modeled `Result` and pressure branches,
not arbitrary panic/unwind. An implementation that can unwind while the parts
are outside `PreparingParts` requires the separately proven owning guard from
Section 8; without it, construction stops before promotion.

## 10. Capacity, backpressure, and fairness

No new numeric capacity is selected here. Preallocated table storage width is
only memory layout; it is not a runtime lease, owning inventory, or proof of
logical zero. The future root must compose existing bounded witnesses with
named parent reservations:

- Quinn actor admission capacity (`max_live_actors` and its exact reservation);
- bounded pre-auth and in-flight bootstrap/task/result ownership;
- owner-session slot capacity spanning pre-auth reservation through reclaim;
- route/directory entry capacity;
- bounded mailbox control and data capacity;
- controller/output, replay, ACK, timer, and terminal-ledger capacity;
- TCP Target socket and pending-effect capacity;
- future UDP packet, datagram, sequence, TTL, expiry, deduplication, and
  backpressure capacity, after its inward Interface is separately accepted;
- accepted-session `SessionFlowId`-keyed flow-terminal tombstone capacity `Z`,
  owned and settled within each session root; and
- if later required by protocol evidence, a distinct bounded parent capacity
  domain for pre-auth/session-identity retention, whose name and quantitative
  bound remain a future architecture gate and cannot reuse `Z`.

The session slot lease is distinct from actor, route, mailbox, scheduler,
Target, and restart-credit leases. In particular, the restart-credit budget `R`
is not a `Reserved` slot count and cannot be reconstructed from this table.

Let `C_session` be configured logical session-slot capacity. Its audited slot
conservation law is:

```text
Vacant
+ PreAuthReserved
+ Bootstrapping
+ AuthenticationPending
+ AuthenticationRejected
+ PreparingSeed
+ PreparingParts
+ Live
+ Terminalizing
+ Reclaimable
+ BootstrapQuarantined
+ Quarantined
== C_session
```

Every non-`Vacant` term owns one exact runtime slot lease. Therefore:

```text
leased_session_slots == C_session - Vacant
```

These equations audit typed phases and leases; they are not independently
editable counters. `Reclaimable` continues to consume a lease until its
whole-session-zero capability is consumed and every runtime reservation is
released. All slots may be logically `Vacant` while their preallocated backing
storage remains allocated.

Within each accepted session, the existing configured capacity `Z` bounds only
the `SessionFlowId`-keyed flow-terminal tombstone population. Its private typed
lease conservation is:

```text
Vacant_Z + Reserved_Z + Retained_Z + Quarantined_Z == Z
```

Only non-`Vacant_Z` terms own flow-terminal tombstones or `Z` leases. Before the
session root may mint `SessionJoinedZero`, it must consume or exact-settle every
such term, leaving no `Z` ownership or lease to move into `Reclaimable` or the
parent. `Vacant_Z == Z` then describes available capacity, not retained logical
inventory.

This is the same accepted-session flow-terminal population `Z` used by the R7
scan-width equation below. It is not process session-slot capacity `C_session`,
and it is not a parent pre-auth/session-identity tombstone domain. If the latter
is ever required, its independent bound, leases, retirement, and false-zero
rules must be frozen in a future architecture gate. A live session with nonzero
`Z`, any in-flight bootstrap, a non-consumed `Reclaimable`, either quarantine
phase, or any future retained parent identity entry blocks whole-server
`OwnerServerJoinedZero`. Checked configuration math must reject overflow before
resources are published.

The already accepted R7 scan envelope remains separate and unchanged:

```text
N = 2 * (F + Z)                         checked add and multiply
Q = 32
turns_per_attempt = ceil(N / Q)
A = S + R                               checked add
max_scan_turns = turns_per_attempt * A  checked multiply
```

For the accepted `F=16, Z=1, S=3, R=2` witness, `N=34`,
`turns_per_attempt=2`, `A=5`, and `max_scan_turns=10`. Static scan capacity is
necessary-only; it does not provide a process-level session slot or runtime
restart-credit Authority.

The custody table participates only in bounded control/lifecycle work. TCP DATA
bytes remain on the per-session actor/controller/`TargetIo` path. Future UDP
packets must remain on their separately accepted per-session path. Neither path
uses a global per-record lookup, full-ledger scan, allocation, or lock.

The server scheduler must use an explicit bounded work budget and resumable
cursor across ready sessions, bootstraps, and shutdown work. A busy session may
not starve another live or terminalizing session. The exact scheduling policy
and capacity product must be frozen and tested before Rust; this document does
not infer it from separately bounded local queues.

## 11. Dependency and Adapter direction

Dependencies point inward:

```text
Main / signal / config
  -> OwnerServerRoot policy and lifecycle
     -> ingress/bootstrap event seam (exact inward Interface not yet selected)
        <- Quinn transport/ingress Adapter
     -> TargetIo (TCP/byte-stream inward Interface)
        <- concrete TCP Target socket Adapter
     -> future UDP inward Interface (not yet named)
        <- future UDP Target Adapter
```

- The Quinn listener is a Humble ingress Adapter. It translates connection and
  bootstrap events; it does not authenticate, commit attach state, or own
  sessions. Authentication and attach commit are `OwnerServerRoot` policy.
- The exact ingress/bootstrap event Interface has not been selected. This file
  does not manufacture a public port or `dyn` seam before source evidence.
- `TargetIo` remains the TCP/byte-stream inward Interface. `MemoryTarget`
  remains its test Adapter. A real TCP Target socket Adapter/factory is a Task 6
  production gap.
- UDP packet/sequence/TTL/datagram/expiry/dedup/backpressure behavior requires a
  separate architecture gate and a still-unnamed inward Interface. `TargetIo`
  does not cover it, and this file does not invent its trait.
- Task 6 ultimately requires the parent to own both TCP and UDP Target
  resources. This sidecar freezes that custody obligation and the existing TCP
  seam only; UDP remains an open architecture gate.
- One concrete Adapter is enough until source evidence demonstrates multiple
  providers with differing behavior. No public trait or `dyn` boundary is added
  from speculation.
- Multiple same-type Quinn ingress instances are instances of one Adapter, not
  a second architectural Adapter or owner.

## 12. Drive, cancellation, and postcommit behavior

`drive_until` borrows the process root and performs bounded lifecycle turns.
It may admit, advance, terminalize, and settle sessions, but all exact owning
values remain in parent slots across an observer yield.

- Dropping a pending observer future ends the borrow only.
- Re-entry resumes the same root, same slots, and same first dispositions.
- A pending pre-auth bootstrap is represented by root-held task/result custody
  before the first await or cancellation point after that custody is created;
  cancellation cannot invoke detached cleanup or erase its exact outcome.
- A session direct join borrows its slot-held physical root; it does not move
  that root into a task.
- Modeled join success/error is retained root-first before an observation
  escapes, as required by the accepted direct-join contract.
- Fatal terminal selection is minted by the root state transition exactly once;
  without that root-minted capability no terminal transaction can begin.
- Raw internal errors do not escape as a second Authority.
- A caught postcommit unwind is sticky and nonzero only when the catch is
  outside the borrow and the same whole root remains in the parent slot.
  Recovery may roll forward from that root; it never reconstructs a seed or
  Running from an error category.
- Arbitrary precommit panic exact-return is not part of this contract. A future
  caught-unwind path needs the exact-parts guard and deterministic proof defined
  in Section 8 before it may be considered recoverable.

## 13. Controlled shutdown contract

Clean shutdown proceeds in this order:

1. Close only the `OwnerServerRoot` admission gate to new candidates. Do not yet
   shut down the Quinn listener or endpoint driver.
2. Drive every bounded in-flight bootstrap to exact rejection, adoption, or
   root-retained `BootstrapQuarantined` disposition, consuming every exact
   bootstrap task/result capability.
3. Close each session mailbox to new work while retaining queued work for drain.
4. Through the same controller/executor, drive `SessionSupervisor` terminal and
   session-expiry effects; then settle FIFO, replay, application ACK, timers,
   controller/output work, TCP Target flows/socket ownership, future UDP Target
   ownership, actors, endpoints, session identity, and every accepted-session
   `SessionFlowId`-keyed flow-terminal tombstone and `Z` lease.
5. Only after that session's `Z` inventory and all other session-root ownership
   are exactly zero, mint `SessionJoinedZero` and move the slot into
   `Reclaimable`. Consuming reclaim releases every remaining parent
   route/directory/mailbox/scheduler/session lease and returns the slot to
   `Vacant`; `Reclaimable` never owns `Z`.
6. If a separately accepted bounded parent pre-auth/session-identity retention
   domain is introduced later, exact-settle it under its own capacity contract;
   it cannot reuse `Z` and must be empty before whole-server conversion.
7. Shut down and join listener termination/release work and the endpoint driver
   last.
8. Convert the consumed server root to `OwnerServerJoinedZero` only when every
   logical owning inventory is zero and every bounded lease domain reconciles
   to its fully free state.

An actor-local joined-zero result is not a whole-root result. Listener
`live == 0` is not a whole-server result. Neither can bypass the remaining
session, Target, ACK, timer, controller, endpoint, or capacity inventories.

If `try_into_joined(self)` cannot prove the final state, it returns
`NotJoined { exact_whole_server_root }`. The caller must continue structured
settlement or fail closed; dropping that returned value is not cleanup evidence.
Preallocated table memory may remain allocated at conversion; clean zero is
about logical Authority, owning inventory, and runtime leases, not allocator
capacity bytes.

## 14. Crash and restart boundary

Controlled observer cancellation, modeled failures, and shutdown are lifecycle
cases owned by this root. A postcommit unwind is root-quarantined only when an
outer parent-preserving catch observes a disposition published before the
unwind-capable turn. An arbitrary precommit panic is not accepted as exact-return
without the proven owning guard; an unguarded gap must not be caught and
normalized to clean zero. An unwind that destroys the root, process crash,
`SIGKILL`, host power loss, and migration of live Target sockets to another
process remain fail-stop non-goals under ADR-0015.

After process restart, a new owner must not accept a stale live-session identity
as proof of resumability. It begins a new authenticated owner epoch and rejects
or re-establishes old claims according to a separately accepted protocol.

Any future requirement for cross-process recovery, durable resumable identity,
or Target-socket transfer requires a separate ADR. It cannot be inferred from
this in-memory ownership graph.

## 15. Falsifiable failure matrix

| Branch | Required evidence | Falsifier / fail-closed result |
|---|---|---|
| Success | One pre-auth capability remains root-owned through bootstrap/authentication; one accepted candidate consumes one seed and publishes one whole physical root | Detached bootstrap, root returned to caller, duplicated slot, or missing Target/controller/handoff ownership |
| Rejection | Every modeled parent gate or exact-parts validation error releases its leases; every modeled post-split error uses `from_parts` to exact-return the same seed; `AuthenticationRejected` exact-settles bootstrap/result custody and releases its slot without consuming `Z` | Seed-specific read before `into_parts`, scalar reconstruction, partial cleanup, published slot, or pre-auth/session identity charged to `Z` |
| Pressure | All fallible reservation/allocation/socket/channel work occurs before final root build; modeled pressure returns the stage's exact aggregate, and returns a seed only after one exists | `Ok(root)` followed by fallible table insertion, invented seed return, leaked route, ACK, nonce, timer, bootstrap task, or socket |
| Fatal | Root-minted first disposition freezes once and retains the exact root/first fault | Raw error escapes, second terminal Authority appears, or slot becomes Vacant |
| Observer cancellation | Dropping `drive_until` leaves the same root, bootstrap task/result, and session-slot custody re-enterable | Bootstrap Drop cleanup, detached session task, lost outcome, or second join/admission |
| Shutdown | Root admission closes first; bootstraps settle; each accepted session exact-settles its flow-terminal `Z` before `SessionJoinedZero`; any future parent identity domain settles separately; listener joins last | Endpoint closed before bootstrap settlement, `SessionJoinedZero` minted with nonzero `Z`, or actor/listener zero accepted while other inventory/leases remain |
| Precommit unwind | No exact-return claim exists until an owning exact-parts guard is deterministically proven | Caught panic across an unguarded parts window, synthetic seed return, or clean zero |
| Unknown postcommit unwind | A disposition is published before the turn; an outer parent-preserving catch retains the same whole root/reservations and records the first fault in `Quarantined` | Catch inside the borrow, root destruction, seed reconstruction, recovery to Running, or clean zero |
| Duplicate ownership | XOR audit proves exactly one parent slot owns each whole root | `Arc`, cloneable token, client/registry/task co-owner, or actor-only extraction |
| Capacity/false zero | Every accepted session proved `Z` zero before reclaim, all session slots are `Vacant`, any future independently bounded parent identity domain is empty, and all other lease domains are free; backing storage may remain | Nonzero session `Z`, `Reclaimable`, bootstrap, parent identity entry, or quarantine exists while a session/server zero capability is minted |
| Ultimate destruction | Production Main consumes or retains `NotJoined` root until structured settlement; ordinary `Drop` is never evidence | Abort, detach, leak, destructor field extraction, or process return with live custody |
| Module deletion | Deleting `OwnerServerRoot` removes the complete owner-server production path, while deleting a listener Adapter leaves domain lifecycle policy intact | Case C protocol remains scattered among Main, listener, callbacks, or session tasks |
| Provenance | Code-aware routing proves exactly one non-test Main-to-root path and no test/B-only/public-legacy bypass | Only cfg(test) construction, no owning slot, or public route still presented as new root evidence |

Any falsifier is a stop, not permission for a local patch.

## 16. Rejected alternatives

### Reuse the listener registry

Rejected. It owns bounded actor-registration accounting, not authenticated
session roots, Target sockets, mailboxes, controller/output state, or their
settlement ledger.

### Reuse `QuinnClientRoot` or `LegacyOwnedUpstream`

Rejected. One has client-side dependency direction; the other deliberately owns
no lifecycle. Public TUN reachability does not create server Authority.

### Promote the current physical root to process root

Rejected. It owns one CurrentOnly session and explicitly excludes listener,
terminal/no-ACK completion, B, runtime credit, public routing, and whole-root
zero. A per-session Module cannot own its process-level siblings.

### Extract actor custody to a parent

Rejected. It separates actor state from handoff, controller/output, Target,
restart-credit, and session settlement. Restoring coherence would require
getters, adopter protocols, callbacks, and a second lifecycle Interface.

### Spawn one whole-root task per session

Rejected. The task becomes a second lifecycle owner and adds spawn/receiver/join
failure states after the fused commit. A dropped handle or nursery is not exact
settlement.

### Add a global registry, `Arc`, copied token, janitor, or leak

Rejected. These copy evidence or defer cleanup instead of preserving the unique
owning value and its exact ledger.

### Put ownership policy in Main

Rejected. Main is a Humble composition Adapter. Teaching it slot phases,
terminal order, or session settlement makes the boundary shallow and leaves a
protocol behind when the owner Module is deleted.

### Generalize Target providers now

Rejected. A real TCP Adapter for the existing `TargetIo` Interface is missing,
and the UDP inward Interface has not been designed. No current source evidence
requires one public provider trait or dynamic dispatch. TCP and UDP must cross
their own accepted inward seams instead of being falsely unified.

## 17. First future RED and implementation gates

This architecture acceptance authorizes no Rust. The next gate is a separately
authorized and reviewed route/privacy plus production-caller implementation-plan
freeze. Only after that gate and separate Rust authorization may the first
production tracer fail on the complete missing seam:

```text
production OwnerServerRoot
+ bounded pre-auth bootstrap/task/result custody
+ bounded authenticated owning slot and runtime leases
+ fused whole-root adoption transaction
+ non-test caller storage from mini_vpn-upstreamd Main
```

It must not be described as one missing method. The current tree lacks the
Module/type, root-retained bootstrap seam, bounded owning table, adoption
transaction, real non-test caller, production TCP Target Adapter, and UDP inward
Interface/Adapter architecture.

The future evidence sequence is:

1. Code-aware provenance RED for exactly one intended production Module route,
   opaque caller Interface, private listener-admission-to-owning-slot seam, and
   non-test Main storage; no test/B-only bypass.
2. A pre-auth tracer proving exact bootstrap/task/result custody is root-owned
   before the first await/cancellation point after admission creates it,
   observer cancellation is re-enterable, and the current detached-cleanup seam
   is not production evidence.
3. Capacity tracers for `C_session` phase/lease conservation, per-session `Z`
   flow-terminal conservation and exact zero before `SessionJoinedZero`,
   `Reclaimable -> Vacant`, all other lease returns, and cross-session fairness
   without DATA-path global locking. If parent pre-auth/session-identity
   retention is later required, its separately frozen capacity gets its own
   tracer rather than reusing `Z`.
4. A fused-adoption RED/GREEN proving `into_parts` precedes all seed-specific
   reads, modeled errors exact-reassemble through `from_parts`, and whole-root
   publication immediately follows the final fallible build.
5. If caught precommit unwind is in scope, first add a deterministic tracer for
   a true phase-tagged exact-parts guard; otherwise it remains an explicit stop.
6. Success, rejection, pressure, fatal, observer-cancellation, shutdown, and
   postcommit-unwind tracers over the same owning slot state machine.
7. A `NotJoined` conversion tracer proving the exact server root is returned,
   not dropped, whenever a bootstrap, live session with nonzero `Z`,
   `Reclaimable`, quarantine, future parent identity entry, or other live lease
   prevents `OwnerServerJoinedZero`.
8. Real TCP Target Adapter acceptance through `TargetIo`; UDP receives its own
   separately reviewed architecture and deterministic gates.
9. Deletion tests proving listener/index Adapters contain no lifecycle policy
   and deleting `OwnerServerRoot` removes all production owner-server behavior.
10. Focused repeats, directly affected regressions, repository provenance,
    format/diff/check, code-growth governance, and independent review.

Before step 1, a separately reviewed implementation plan must freeze exact
allowed files, symbols, Writer ownership, expected line counts, and TDD filters.
None are invented here.

The first unexpected compile/test failure, need for an unreviewed visible
Interface, unbounded capacity, whole-root task transfer, post-build fallible
insertion, or third ownership protocol stops implementation and returns to
architecture review.

## 18. Still-open prerequisite sequence

Although this architecture-only proposal is accepted, these remain independent
gates:

1. concrete Rust route/privacy and non-test caller placement;
2. ownership-returning/retaining pre-auth bootstrap/task/result seam;
3. real bounded owner-session table, per-session flow-terminal `Z` settlement
   before `SessionJoinedZero`, reclaim, and fair control scheduler; if protocol
   evidence requires parent pre-auth/session-identity retention, its distinct
   bounded domain and retirement are a separate prerequisite;
4. real TCP Target socket Adapter/factory through `TargetIo`;
5. separately accepted UDP Target inward Interface and Adapter;
6. initial-A Authority absorption into the new root;
7. production terminal preservation via root-minted outcome;
8. root-owned no-ACK whole-session finalizer;
9. authenticated physical B topology and evidence;
10. runtime restart credit after controller progress and hint issuer share one root;
11. remaining R7a-d physical composition and no-oracle byte ledger;
12. whole-server exact-zero and Case C implementation acceptance;
13. only then, a separately authorized public TUN and external acceptance plan.

VPS, WAN, throughput, M2, and public-route claims remain forbidden now.
The isolated real-loopback `ResidualOwnership { messages: 1, bytes: 0 }` fixture
debt also remains separate and is not repaired or waived by this proposal.

## 19. Readiness and acceptance

Current Case C implementation readiness remains **4/10**:

- the per-session CurrentOnly constructor and living-root direct join exist as
  accepted necessary-only local evidence;
- the correct whole-root ownership direction is known; but
- the process parent, Rust route, pre-auth custody seam, authenticated table,
  real TCP Target Adapter, UDP inward Interface, caller reachability,
  cross-session scheduler, and whole-server settlement do not exist.

Independent review scores this frozen architecture boundary `10/10` for
architecture-only correctness. Implementation readiness remains `4/10`.
Reaching `10/10` implementation readiness requires executable evidence for:

- one concrete Module route with legal minimal visibility and no Interface
  diffusion;
- one non-test Main-to-root composition path;
- root-retained pre-auth bootstrap/task/result custody before the first
  await/cancellation point after that custody is created;
- typed `C_session` slot and runtime-lease conservation, per-session
  `SessionFlowId` flow-terminal `Z` conservation with exact zero before
  `SessionJoinedZero`, and `Reclaimable -> Vacant`;
- a separately frozen bounded parent identity-retention domain and zero proof if
  future protocol evidence requires one, without reusing `Z`;
- fused exact-return adoption into a parent-owned whole-root slot;
- a real TCP Target Adapter and a separately accepted UDP inward Interface/
  Adapter, each with exact ownership and shutdown;
- bounded cross-session fairness and backpressure math;
- deterministic proof of an exact-parts guard before any caught precommit unwind
  claim;
- all failure-matrix branches through one settlement model;
- whole-server `OwnerServerJoinedZero | NotJoined { exact root }` conversion;
- code-aware provenance, deletion tests, deterministic repeats, governance, and
  independent architecture/code review.

This file is **ACCEPTED / ARCHITECTURE-ONLY / RUST NOT AUTHORIZED**. It selects
the required parent role but does not close Case C; all implementation-readiness
items above remain open.

## 20. Stop rule

Stop and return to architecture if any implementation would require:

- exposing or re-exporting the physical root, seed, actor, or custody;
- adding `submit_leg`, a custody getter, a callback protocol, or a second join;
- moving a whole root into a spawned per-session task;
- using `Option`/empty slots, copied inventory, `Arc`, abort, detach, leak, or
  `Drop` as settlement;
- splitting successful output-root construction, CurrentOnly construction, and
  the `PreparingParts -> Live` publication into separately observable or
  unwind-capable operations;
- reading seed-specific state before consuming the real `into_parts` seam, or
  claiming arbitrary precommit panic exact-return without an owning guard;
- reaching the first bootstrap await/cancellation point after admission without
  first publishing root-owned task/result custody, or using detached bootstrap
  cleanup as settlement evidence;
- placing DATA records on a global custody-table/lock path;
- treating preallocated table width or copied inventory as a runtime lease;
- mixing session-slot capacity `C_session` with accepted-session flow-terminal
  capacity `Z`, charging pre-auth/session-identity retention to `Z`, minting
  `SessionJoinedZero` while any session `Z` ownership or lease remains, treating
  `Reclaimable` as a `Z` owner, or inventing an unbounded parent identity
  registry/queue/retry budget;
- claiming `TargetIo` covers UDP or inventing an unreviewed UDP trait;
- treating actor/listener zero as whole-root/server zero;
- claiming process-crash recovery without a new ADR; or
- extending into public TUN, external acceptance, or another R7 behavior.

## 21. Source links

Architecture and current evidence:

- [Current status](../current/STATUS.md)
- [Project domain language](../../CONTEXT.md)
- [Product rules](../../Rules.md)
- [ADR-0015: path-diverse resumable upstream](../adr/0015-path-diverse-resumable-upstream.md)
- [Knife16 architecture specification](2026-08-18-knife16-path-diverse-resumable-upstream-architecture-spec.md)
- [Knife16 implementation plan](2026-08-18-knife16-path-diverse-resumable-upstream-implementation-plan.md)
- [Post-extraction owner-root cutover amendment](2026-09-02-knife16-r7-post-extraction-owner-root-cutover-amendment.md)
- [Join-custody amendment](2026-09-03-knife16-r7-current-only-join-custody-amendment.md)
- [Direct-join amendment](2026-09-03-knife16-r7-current-only-direct-join-amendment.md)
- [Direct-join local result](2026-09-03-knife16-r7-current-only-direct-join-local-results.md)

Relevant source seams:

- [`src/main.rs`](../../src/main.rs)
- [`src/lib.rs`](../../src/lib.rs)
- [`src/owned_upstream/mod.rs`](../../src/owned_upstream/mod.rs)
- [`owner_initial.rs`](../../src/owned_upstream/quinn_leg_composition/owner_initial.rs)
- [`physical_session_root.rs`](../../src/owned_upstream/quinn_leg_composition/owner/physical_session_root.rs)
- [`continuation.rs`](../../src/owned_upstream/quinn_leg_composition/owner/continuation.rs)
- [`quinn_leg_actor.rs`](../../src/owned_upstream/quinn_leg_actor.rs)
- [`session.rs`](../../src/owned_upstream/session.rs)
- [`supervisor.rs`](../../src/owned_upstream/supervisor.rs)
- [`owner_target.rs`](../../src/owned_upstream/owner_target.rs)
- [`target.rs`](../../src/owned_upstream/target.rs)
- [`quinn_client_root.rs`](../../src/owned_upstream/quinn_client_root.rs)
- [`legacy.rs`](../../src/owned_upstream/legacy.rs)
- [`client_tun.rs`](../../src/client_tun.rs)

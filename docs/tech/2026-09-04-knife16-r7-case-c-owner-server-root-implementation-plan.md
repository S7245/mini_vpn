# Knife16 R7 Case C owner-server-root implementation plan

Date: 2026-09-04

Status: **ACCEPTED / PLAN-ONLY / RUST NOT AUTHORIZED / PREREQUISITES OPEN**

This sidecar converts the accepted
[Case C owner-server-root architecture](2026-09-04-knife16-r7-case-c-owner-server-root-architecture.md)
into a conditional implementation sequence. It freezes the Rust route and
privacy direction, but two unresolved progress Interfaces still prohibit Rust:
asynchronous TCP Target open and process-level ready-session scheduling.
Separately, the current standalone physical-root constructor cannot enter P5B
until the Rust-zero fused-publication contract gate proves a compilable parent
slot Seam.

Independent plan review closed the preceding six P1 and one P2 findings. The
same Reviewer returned P0/P1/P2 `0/0/0`, Approve. This acceptance freezes only
the conditional implementation sequence and its stop rules; it does not accept
or authorize any Rust implementation.

Nothing in this document accepts an implementation, closes Case C, changes the
public TUN path, or authorizes Cargo, external runs, commit, or push.

## 1. Decision

The future production chain is:

```text
src/bin/mini_vpn-upstreamd.rs           Humble composition Adapter
  -> mini_vpn::{OwnerServerRoot, ...}   crate-root public facade
     -> quinn_leg_composition::owner::server_root
                                        deep process-lifecycle Module
        -> private bounded bootstrap/session/scheduler/TCP children
        -> whole OwnerPhysicalSessionRoot<TcpTarget...> in each live slot
```

The existing [`src/main.rs`](../../src/main.rs) remains the client/probe binary
and is not changed. The deep implementation belongs in the new private sibling
`owner/server_root.rs`,
beside the existing
[`physical_session_root.rs`](../../src/owned_upstream/quinn_leg_composition/owner/physical_session_root.rs).
There is no physical-subtree Move: it has no behavior or privacy benefit and
would create provenance, documentation, and import churn.

`OwnerServerRoot` is the concrete process form of the existing
**Upstream session owner** term in [`CONTEXT.md`](../../CONTEXT.md). Main is an
outer Framework/Driver plugin. The server root owns policy and lifecycle; the
Quinn listener and future TCP socket implementation remain Adapters.

## 2. Source-grounded facts

The current tree establishes these constraints:

- [`src/lib.rs`](../../src/lib.rs) declares `owned_upstream` as `pub(crate)`.
  It currently exports no owner-server facade.
- [`owned_upstream/mod.rs`](../../src/owned_upstream/mod.rs) declares
  `quinn_leg_composition` as `pub(crate)`. This file is not needed in the new
  facade chain and must remain unchanged.
- [`quinn_leg_composition.rs`](../../src/owned_upstream/quinn_leg_composition.rs)
  declares private `owner` and `owner_initial` Modules. It is a grandfathered
  9,447-line production file and cannot grow.
- [`owner.rs`](../../src/owned_upstream/quinn_leg_composition/owner.rs) declares
  private `physical_session_root`. It is a grandfathered 2,062-line production
  file and cannot grow.
- `OwnerPhysicalSessionRoot<T>`, its build failure, and its constructor are
  private to `physical_session_root.rs`. Current non-test callers are zero;
  only `tests/current_only.rs` constructs and drives it.
- `QuinnOwnerInitialPhysicalRootSeed<T>` is already `pub(super)` in
  `owner_initial.rs`. Its fields remain private; reversible inspection is only
  through consuming `into_parts` paired with `from_parts`.
- [`quinn_leg_actor.rs`](../../src/owned_upstream/quinn_leg_actor.rs) is 7,001
  lines. `QuinnLegActorBootstrap::start_with_clock` moves the bootstrap into
  `runtime.spawn`, communicates through a oneshot, and awaits that receiver.
  `ServerBootstrapHandoff::Drop` and `QuinnLegActorBootstrap::Drop` spawn
  cleanup whose final result is not retained by a process root. This is not
  Case C custody evidence. The outer `QuinnLegActorBootstrap` is already
  parent-declared and `pub(crate)`; it is the legal type to deepen for sibling
  custody without exposing the future private child Module.
- `run_server_bootstrap` currently carries one of the seven grandfathered
  `clippy::too_many_arguments` allowances owned by `quinn_leg_actor.rs`.
  Governance forbids transferring that allowance to a new source path. The
  argument seam must therefore converge and the actor allowance inventory must
  shrink before the function can move to a child Module.
- [`TargetIo`](../../src/owned_upstream/target.rs) has synchronous `open` and
  `TargetOpenCompletion` has no Pending state. The `PeerOpenRequested` arm in
  [`owner_target.rs`](../../src/owned_upstream/owner_target.rs) immediately maps
  `Opened | AlreadyOpen` to `OpenResultCode::Opened` and publishes completion.
  A real Tokio TCP/DNS connect cannot obey this contract without blocking,
  optimistic success, or detached ownership.
- [`SessionOwnerMailbox`](../../src/owned_upstream/session.rs) has an eight-item
  control-burst rule inside one session. `OwnerTargetExecutor` also bounds one
  incremental effect burst at three. Neither supplies process-level dynamic
  fairness, a ready-set lease, a persistent cross-session cursor, or a lost-
  wakeup protocol.
- Its async `send_command` moves the command into `Sender::send(...).await`;
  cancellation can therefore destroy a future that owns the command. Existing
  `try_send_*` paths do return the exact command on `Full` or `Closed`, but no
  process-root pending-send custody currently surrounds either path.
- Cargo auto-discovers `src/bin/*.rs`; no `Cargo.toml` edit is required for the
  dedicated future binary.

The executable governance currently freezes new production/test leaf/test
index limits at 1,500/800/300 lines. Grandfathered parents are shrink-only, and
the Feature Writer never edits their inventory.

## 3. Module depth and ownership placement

The future private tree is conditional but frozen in direction:

```text
owner
├── physical_session_root.rs            per-session whole-root Authority
└── server_root.rs                       process-level Authority and Interface
    └── server_root/
        ├── bootstrap.rs                 root-retained pre-auth task/result
        ├── session_table.rs             fixed-width owning slots and leases
        ├── scheduler.rs                 bounded ready ownership and cursor
        └── tcp_target.rs                future concrete TCP Adapter/factory
```

Each child is private and must be a deep Module with a compact module contract:
what it owns, what it does not own, invariants, entry symbols, and test route.
Deleting `server_root` must remove the complete production owner-server
lifecycle. Deleting a listener or TCP Adapter must leave owner policy intact.

The tree is established only as one compiling vertical boundary after P1:
`owner.rs` declares private `server_root`, `owner/server_root.rs` declares its
first private child, and that child supplies the first owned behavior. A child
may not be created before its parent route, and an empty root/scaffold is not an
accepted intermediate state. The `owner.rs` edit must be rustfmt-stable and
net-zero or shrinking; if the declaration cannot satisfy its exact ratchet in
the same coherent change, that phase stops before writing.

The session table uses fixed-capacity storage and phase-owned values. It may not
represent ownership with an empty `Option`, cloneable `Arc` token, copied
inventory, global registry, per-session whole-root task, detach, abort, leak, or
`Drop` cleanup. A live slot directly owns the whole physical root.

DATA stays on the session actor/controller/Target path. The process root drives
bounded control and lifecycle turns; it does not add a full-table lock, scan,
or allocation to every DATA record.

## 4. Caller Interface

Only three lifecycle entries may cross the crate-root facade:

```text
OwnerServerRoot::bind(...)
OwnerServerRoot::drive_until(&mut self, ...)
OwnerServerRoot::try_into_joined(self)
```

The exact argument shapes remain blocked by the Target-open progress design.
The semantic contract is fixed:

1. `bind` creates or takes ownership of concrete resources and either returns
   one closed root or an exact modeled start failure after settling everything
   it acquired.
2. `drive_until` borrows the root and returns only borrowed or value-only
   observation. Observer cancellation ends the borrow; the same root remains
   re-enterable.
3. `try_into_joined` consumes the root. Its `NotJoined` branch carries the
   exact whole root; it never substitutes an error category or inventory copy.

Conditional facade symbols are:

- `OwnerServerRoot`;
- `OwnerServerConfig`;
- `OwnerServerStartError`;
- `OwnerServerDriveObservation`;
- `OwnerServerJoinedZero`; and
- `OwnerServerJoinDisposition`, whose `NotJoined(OwnerServerRoot)` variant is
  an owning, non-Clone/non-Copy return carrier.

Config, start error, drive observation, and joined-zero evidence are value-only
supporting types with private fields. Every value that owns lifecycle Authority
is non-Clone/non-Copy. No public signature names `TargetIo`, Quinn, Tokio,
`OwnerPhysicalSessionRoot`, a seed, actor, custody, or `JoinHandle`. There is no
public `submit_leg`, getter, callback protocol, or second join Interface.

`OwnerServerStartError` is value-only only while every bind failure has already
exact-settled all acquired resources. If a future branch must retain a listener,
task, slot, or socket Authority, value-only failure is invalid and a separately
reviewed owning start disposition is required before implementation.

`OwnerServerConfig` must have one public value-only construction and validation
route so the dedicated binary can create it without field access. Its exact
shape remains blocked by P1 and is frozen with the facade in deferred final
integration. That supporting constructor is not a lifecycle entry and may not
acquire a socket, task, slot, or other runtime Authority.

Adding a fourth lifecycle entry is an architecture stop.

## 5. Shortest compiling visibility chain

The only permitted public path is:

```text
owner/server_root.rs
  pub facade symbols, private fields and children
owner.rs
  private mod server_root;
  pub use server_root::{facade symbols};
quinn_leg_composition.rs
  pub use owner::{facade symbols};
src/lib.rs
  pub use owned_upstream::quinn_leg_composition::{facade symbols};
src/bin/mini_vpn-upstreamd.rs
  use mini_vpn::{facade symbols};
```

This is a deferred-final route, not a P0–P7 implementation action.

Rust does not allow a restricted re-export to be widened one scope at a time.
Therefore the selected facade items are genuinely `pub` and each private
Module performs a `pub use`; the private Modules and crate-private path remain
unnameable outside the library. The external API is only the crate-root facade.
The Integrator has separately verified this Rust visibility rule with a
standalone compiler characterization.

`owned_upstream/mod.rs` is deliberately absent from the re-export chain. A new
top-level `owner_server.rs` wrapper is forbidden because it adds a shallow
pass-through Module without owning policy.

The narrow physical-root change, when its caller exists, is owner-scope only:

- `OwnerPhysicalSessionRoot<T>` becomes `pub(super)` only when the parent slot
  that stores it is compiled in the same coherent change;
- current `from_initial_seed` remains child-private and necessary-only because
  its standalone `Result<Self, _>` is not the production publication Seam;
- only the P5A-approved prepared-parts/publication symbols receive the minimum
  owner scope needed by their exact sibling caller; and
- any later borrow/consuming operation is promoted only with its exact sibling
  caller, never pre-emptively.

This scope is no wider than `quinn_leg_composition::owner`. Seed fields, actor,
current custody, phase, inventory internals, controller, and `JoinHandle` remain
unnamed outside their owning Modules. There is no physical-root re-export.

Visibility alone does not make the current `from_initial_seed` production-safe:
it returns a nameable, unpublished `Result<OwnerPhysicalSessionRoot<_>, _>`.
Before P5B behavior, a separate Rust-zero compile-shaped contract gate must
freeze one consuming `PreparingParts` publication capability already owned by
the parent slot. Its fused operation:

- consumes that capability and the exact seed;
- owns an exact-parts guard during precommit inspection;
- on modeled rejection or pressure, returns the same preparing capability,
  reassembled seed, reservations, and exact cause;
- calls output-root construction as its final fallible operation; and
- on success writes `Live { exact whole physical root }` through the same slot
  capability and returns only value observation, never `Ok(root)`.

The gate must include compiler-shaped private type/ownership sketches and prove
that neither the physical Module nor the parent needs a callback, generic
publisher trait, second Adapter seam, public slot type, or physical-root
re-export. If no such shape compiles without reversing the dependency rule, P5B
does not start and the architecture must be revised.

## 6. Bootstrap custody prerequisite

Bootstrap convergence has three separately reviewed promotion boundaries. It
may not be compressed into one mixed patch.

First, a test/provenance-only Move creates private bootstrap leaves and moves
the existing characterization blocks without changing test bodies, assertion
counts, or behavior. Imports, compact Module docs, and route declarations sit
outside the hashed moved blocks. Because a child Module changes Rust's fully
qualified test names, the Move records an exact old-to-new FQ mapping and uses
the new names after movement; it never claims path identity. Both oversized
parents strictly shrink, and the Integrator lowers their line ratchets before
production work resumes.

Second, a behavior-preserving parameter-object convergence remains in
`quinn_leg_actor.rs`. It replaces the multi-argument
`run_server_bootstrap` call/signature with one private, non-Clone aggregate,
preserves every field identity and evaluation/order boundary, and removes the
function's existing `clippy::too_many_arguments` allowance. The production
parent must be net-zero or shrink. If the aggregate cannot satisfy that bound,
an independently characterized pure headroom Move is required first; unrelated
compression cannot be mixed into this step. Existing focused behavior and the
newly moved tests characterize the seam. After they pass, the Feature Writer
reports the exact line and allowance shrink, and the Integrator lowers the
actor allowance inventory from seven to six in the same promoted change.

Third, and only after that allowance removal is accepted, a behavior-preserving
Move extracts the current bootstrap Implementation from oversized
`quinn_leg_actor.rs` into private `quinn_leg_actor/bootstrap.rs`. The stable
parent Interface anchors remain listener admission and the
`QuinnLegActorBootstrap` declaration. The moved closure is limited to:

- the private parameter aggregate introduced by the preceding convergence;
- `ServerBootstrapHandoff`, its inherent impl, and `Drop`;
- `QuinnLegActorBootstrap::{start, start_with_clock}` and its `Drop`;
- `run_server_bootstrap`; and
- only private helper bodies proved by a symbol-use closure to belong solely to
  those entries.

No behavior, effective visibility, ordering, cleanup, or result semantics
change during that Move. No allowance moves with it. Existing tests and
code-aware provenance characterize it before and after. The parent must
strictly shrink; the Integrator alone lowers its line ratchet.

Only after the Move is reviewed may a separate behavior slice deepen the
existing parent-declared `QuinnLegActorBootstrap` into the move-only phase
capability stored by the root. This is the selected sibling-nameable path:

- the existing `pub(crate)` outer type remains declared in
  `quinn_leg_actor.rs` and is the only actor bootstrap type named by
  `owner::server_root`;
- private phase Implementation remains in `quinn_leg_actor/bootstrap.rs`; an
  internal state type may be `pub(super)` only so the actor parent can name its
  field;
- there is no task-type re-export, public child Module, second bootstrap type,
  cloneable handle, or cleanup token; and
- the actor parent and child are both explicit P4 files, with the grandfathered
  parent net-zero or shrinking.

The parent root publishes that same phase capability into a bounded pre-auth
slot before the first subsequent await/cancellation point. Dropping a
`drive_until` observer leaves the same task/result phase in that slot and
re-entry resumes the same outcome.

The current oneshot, unclaimed handoff Drop, and detached cleanup are negative
characterization evidence. They are not production custody or settlement.

## 7. Hard blocker one: asynchronous TCP Target open

The current synchronous `TargetIo::open` cannot honestly represent Tokio DNS
and TCP connect progress. These substitutes are forbidden:

- blocking the owner thread;
- publishing `OpenResultCode::Opened` before a socket exists;
- detaching a connect task from exact flow/session/root custody; or
- treating a copied flow ID or inventory count as the open Authority.

Before any Rust Move or route tracer, one architecture-only amendment must
freeze a split-phase open progress Seam:

- one move-only open request/lease and exact Target/session/flow correlation;
- a bounded start/admission point before side effects;
- Pending versus exact completion ownership;
- a poll/drive rule that never consumes readiness without retaining progress;
- observer cancellation and re-entry;
- duplicate completion and late completion rejection;
- flow-local rejection versus adapter-fatal classification;
- terminal, session shutdown, and whole-server shutdown settlement; and
- exact socket/DNS/task/result inventory and backpressure.

This plan does not select method signatures or invent a public provider trait.
The accepted amendment must prove whether the existing inward Interface is
extended or a deeper private progress capability is used, and it must preserve
the dependency direction from policy to concrete TCP Adapter.

UDP is not covered by `TargetIo`; its datagram/sequence/TTL/expiry/dedup and
backpressure Interface remains a separate later architecture gate.

## 8. Hard blocker two: process-level ready scheduling

Per-session burst bounds do not prove fairness across a dynamic process table.
The same next architecture amendment must freeze:

- fixed `C_session` owning table capacity and phase-specific leases;
- one generation-safe, deduplicated ready ownership per schedulable class;
- a persistent round-robin cursor owned by `OwnerServerRoot`;
- bounded `B_total`, `B_class`, and `B_session` work budgets;
- the protocol between clearing a ready lease and observing newly published
  work, so no wakeup can be lost;
- bounded admission pressure rather than an unbounded ready queue;
- shutdown priority that cannot starve live settlement; and
- a derivable worst-case service bound in control/lifecycle turns.

It must close both sides of publication. Existing async
`SessionOwnerIngress::send_command` moves the exact command into
`mpsc::Sender::send(...).await`; cancellation can therefore destroy the sending
future while it owns the command. Before the first await, the accepted design
must instead place the command in one parent-owned pending-send capability and
choose one of two exact outcomes: a nonblocking `try_send_*` transfers it to the
bounded mailbox, or `Full`/`Closed` returns the same command to that capability.
If asynchronous waiting remains necessary, the root-owned capability—not an
observer future—retains the command through cancellation. Unknown send
disposition becomes sticky nonzero quarantine. A receiver-side ready lease
cannot substitute for this send-side Authority.

The selected direction is root-local bounded round-robin over owned ready
leases. This document intentionally supplies no numbers or speculative method
signatures: workload and existing capacity evidence must determine them in the
next gate.

The following are rejected as schedulers:

- `tokio::select!` branch-ready order;
- sleep or periodic full-table polling;
- sequentially awaiting each session inbox;
- spawning one task that owns each whole session root; or
- a global queue/lock on the DATA path.

## 9. Rejected placement and Interface alternatives

| Alternative | Rejection |
|---|---|
| Add server mode to current `src/main.rs` | Mixes client/probe and process-owner composition; Main stops being Humble. |
| Put implementation in a top-level sibling `owner_server.rs` | Creates a shallow wrapper and a second place for lifecycle policy. |
| Add a public provider trait or `dyn TargetIo` | Exposes an unproven volatility boundary and adds dynamic dispatch before a second provider exists. |
| Put process root inside `physical_session_root` | Reverses ownership: one session Module cannot own the process table and other sessions. |
| Reuse listener, client root, mailbox, or supervisor | Their actual Authority/lifetime is transport, client, sender, or one-session policy—not process custody. |
| Move the physical-root subtree | Adds no behavior or privacy leverage and expands provenance/docs churn. |
| Use a whole-root spawned task or detached janitor | Splits Case C Authority and makes observer cancellation/destruction unverifiable. |

The deletion test favors the selected placement: remove `server_root` and all
process policy disappears without changing the physical-session implementation;
remove Main and only executable composition disappears.

## 10. Conditional implementation sequence

Every phase below is **not authorized** until its predecessor has reviewed
evidence and the user explicitly opens its scope.

### P0 — this sidecar and independent review

Freeze route, privacy, blockers, failure tree, and governance. No Rust.

### P1 — Target-open + ready-scheduler progress Seam

Create one architecture-only amendment with zero Rust. It must close Sections
7 and 8 together because an asynchronous completion without bounded ready
ownership is not serviceable, while a scheduler without exact pending work has
no honest capacity domain. It also freezes send-side exact command custody,
`Full`/`Closed` return, observer cancellation, and sticky unknown disposition.

This is the smallest next gate. Until it is accepted, no provenance RED, Move,
scaffold, route, or implementation is authorized.

### P2 — bootstrap governance convergence and preparatory Move

Use three green-to-green subphases in this exact order:

1. P2T moves bootstrap tests and provenance into private leaves, with hashed
   test bodies mechanically identical, both parents strictly shrinking, and an
   exact old-to-new FQ command map recorded at the Move boundary.
2. P2A replaces the existing `run_server_bootstrap` argument list with one
   private aggregate, preserves exact identities/order, and removes its
   grandfathered allowance while the production parent remains net-zero or
   shrinks. If that cannot be done honestly, stop and freeze a separate
   headroom-extraction packet before changing behavior.
3. P2B moves the now allowance-free bootstrap closure into the private child.

Behavior and effective visibility stay identical throughout P2. Feature
Writers report every exact shrink; the Integrator alone updates line and
allowance inventories at each coherent boundary.

### P3 — compiling root route, Target-open, and scheduler TDD

Use separate vertical tracers and separate minimum GREENs in this order:

1. establish `owner.rs -> server_root.rs -> scheduler.rs` in one compiling
   behavior-bearing boundary; no child-first or empty scaffold state exists;
2. retain one exact producer command in root-owned custody until nonblocking
   transfer, exact `Full`/`Closed` return, or sticky quarantine;
3. retain one deduplicated generation-safe ready lease through cursor handoff;
4. prove the derived per-class/per-session worst-case service bound and no lost
   wakeup at the clear/recheck boundary; and
5. retain one exact TCP open progress capability through Pending/completion,
   cancellation, rejection, fatal, and shutdown.

Target-open and scheduler behavior may share the accepted progress vocabulary,
but one Writer owns each overlapping Module and they do not edit the same file
concurrently. The first scheduler slice explicitly owns the net-zero/shrinking
`owner.rs` route edit plus the root and first child; later P3 slices do not
reopen that parent.

### P4 — root-retained bootstrap custody and bounded table

Introduce private `OwnerServerRoot` internals, fixed owning slots, and
deepen the existing `QuinnLegActorBootstrap` as specified in Section 6. Publish
that exact phase/task/result custody before the first cancellable await after
admission. Lock observer cancellation/re-entry, rejection, pressure, fatal,
shutdown, quarantine, and lease conservation. This phase includes the actor
parent and private bootstrap child; it cannot invent a sibling-inaccessible
task type.

### P5A — fused-publication compile-shape gate

Before repository Rust changes, review one Rust-zero contract packet for the
`PreparingParts` capability described in Section 5. The packet freezes exact
private ownership shapes, success/failure return types, dependency direction,
and the absence of an unpublished `Ok(root)`. A standalone compiler sketch may
characterize privacy and moves, but it is not project implementation evidence.
Any required callback, generic publisher trait, second Adapter seam, public
slot, or dependency from physical policy back to Main is a stop.

### P5B — fused whole-root adoption

The parent reserves every fallible destination resource, owns the accepted
seed, consumes `into_parts`, validates exact parts, and reconstructs the seed on
every modeled precommit error. `QuinnOwnerControllerOutputRoot::new` remains the
final fallible root-build operation. Its success is fused, with no await,
allocation, callback, logging, validation, or `Result`, into CurrentOnly and a
single whole-root move to `Live`.

The operation consumes the parent-owned publication capability. Success writes
the slot and returns value-only observation; no standalone `Ok(root)` may
escape for later insertion.

### P6 — private Case C custody integration

Integrate P3–P5B behind the private root and prove bounded parent ownership:
bootstrap, pending command, ready lease, Target-open progress, accepted seed,
and whole physical root each have one owning phase. This is production Rust but
remains private and test-driven. It does not export the facade, create the
dedicated binary, mint `SessionJoinedZero`/`OwnerServerJoinedZero`, or claim
Case C completion.

### P7 — Case C custody-only review boundary

Review the bounded parent-custody slice across success, modeled rejection,
pressure, fatal, observer cancellation, precommit quarantine, postcommit
quarantine, and shutdown initiation. An attempted shutdown with a retained live
session remains explicitly nonzero and keeps the same private root; no clean
server conversion is available yet.

Run full deterministic evidence, repeat gates, provenance, governance, code
review, and a dated **necessary-only custody** result. This still does not
authorize a public facade, production binary, public TUN, or external
acceptance.

### Deferred final integration — outside P0–P7

Initial-A absorption, production terminal preservation, root-owned no-ACK,
authenticated physical B, runtime restart credit, remaining R7a-d ledgers, real
TCP acceptance, and the separately designed UDP Interface must close first.
Only then may a separately authorized sequence:

1. prove each session's `Z` and every other session/root lease exactly zero,
   mint `SessionJoinedZero`, reclaim every slot, and settle listener/endpoint;
2. implement `OwnerServerJoinedZero | NotJoined { exact root }`;
3. export the three-entry facade through Section 5 and add
   `mini_vpn-upstreamd` as its only non-test caller; and
4. run code-aware route/deletion provenance and request Case C implementation
   acceptance.

Until that sequence, facade symbols in Section 4 are frozen future Interface
names only. A route tracer must eventually fail on the complete missing
chain—root, owning table, fused adoption, zero/NotJoined conversion, facade,
and non-test caller—not pretend one method is the only missing seam.

## 11. Conditional files, symbols, and size budgets

These are planning candidates, not a write authorization.

| Phase | Candidate files and exact symbols | Predicted bounded size |
|---|---|---|
| P1 | new `docs/tech/2026-09-04-knife16-r7-target-open-ready-scheduler-amendment.md` | architecture-only, under 700 lines |
| P2T tests | `quinn_leg_actor/tests/mod.rs`; new private `tests/bootstrap_custody.rs`; `knife16_owned_leg_provenance.rs`; new private `knife16_owned_leg_provenance/bootstrap.rs` | parents strict shrink; each leaf 250–780 |
| P2A production | `quinn_leg_actor.rs`; private bootstrap parameter aggregate; `run_server_bootstrap` and its sole call | parent net-zero/shrink; allowance inventory 7→6 |
| P2B production | `quinn_leg_actor.rs`; new private `quinn_leg_actor/bootstrap.rs`; allowance-free closure listed in Section 6 | parent strict shrink; child 250–650 |
| P3 scheduler/root | `owner.rs` private route; new `owner/server_root.rs`, private `server_root/scheduler.rs`, `server_root/tests/mod.rs`, and `tests/ready_scheduler.rs` | owner parent net-zero/shrink; root 250–500; scheduler 300–650; leaf 250–650 |
| P3 Target | `target.rs::TargetIo`, `TargetOpenCompletion`; `owner_target.rs` `PeerOpenRequested`/completion path; future private `server_root/tcp_target.rs` | old parents net-zero/shrink; child 300–700 |
| P4 | `quinn_leg_actor.rs`; private `quinn_leg_actor/bootstrap.rs`; `server_root.rs`; new `server_root/bootstrap.rs`, `session_table.rs`, `tests/bootstrap_custody.rs` | actor parent net-zero/shrink; production children 250–700; leaf 300–750 |
| P5A | new `docs/tech/2026-09-04-knife16-r7-fused-parent-publication-contract.md` | Rust-zero contract, under 500 lines |
| P5B | `physical_session_root.rs` exact owner-scope construction symbols from Sections 5 and 10; `server_root.rs`; `session_table.rs`; `tests/fused_adoption.rs` | physical root net-zero/shrink; root remains under 1,500; leaf 300–700 |
| P6 | `server_root.rs`, existing private children, and focused integration leaves | every production file under 1,500; each leaf 300–750 |
| P7 | P6 files plus provenance and a dated necessary-only result | code files net-zero; result remains compact |
| Deferred final | `owner.rs`, `quinn_leg_composition.rs`, `lib.rs`, new `src/bin/mini_vpn-upstreamd.rs`, `server_root.rs`, settlement children, and new `tests/knife16_owner_server_provenance.rs` | grandfathered parents net-zero/shrink; lib 45–60; binary 40–120; each leaf 250–750 |

The final `server_root.rs` is predicted at 900–1,400 lines; each private child
must remain below 1,500. Every new test leaf stays below 800 and every test
index below 300. No new `too_many_arguments` or warning allowance is permitted.
In particular, P2B may not move the existing actor allowance to its child or
add that path to the governance inventory.

P3 does not authorize direct behavior growth in oversized `target.rs` or
`owner_target.rs`. Once P1 freezes the exact progress Interface, a symbol-first
slice must prove whether the change is net-zero/shrinking. If it is not, tests
first characterize an independently coherent private-child extraction, the
Move is promoted separately, and only a later slice changes behavior. The
future child path and symbol closure are not invented before P1.

The first P3 scheduler/root slice must establish its complete compiling Module
route in one change. Its `owner.rs` declaration is net-zero/shrinking in that
same rustfmt-stable diff; no child-only predecessor and no unrelated text
compression is permitted.

The deferred-final Writer must prove each `owner.rs` and
`quinn_leg_composition.rs` route edit is independently rustfmt-stable and
net-zero/shrinking. A previously promoted shrink creates no banked headroom
because governance follows the new exact count. If no route-specific net-zero
layout exists, stop and revise facade placement; do not mix an unrelated Move,
delete documentation, or compress behavior to buy lines. `src/main.rs`,
`owned_upstream/mod.rs`, and the physical subtree path are outside
deferred-final scope.

Single-Writer rule:

- one Writer owns `server_root` and its children during an Authority change;
- Target and scheduler Writers may work disjointly only after P1 freezes their
  shared progress contract;
- no parallel Writer edits `owner.rs`, `quinn_leg_composition.rs`, actor parent,
  or a shared test/provenance parent; and
- reviewers remain read-only.

## 12. Conditional exact TDD and provenance filters

Names below are reserved for future tracers; they do not exist unless noted.

P2T starts from these pre-Move identities:

- `owned_upstream::quinn_leg_actor::tests::cancelled_start_caller_leaves_registered_bootstrap_cleanup_to_completion`;
- `...::admitted_slow_bootstrap_does_not_head_of_line_block_a_second_connection`;
- `...::abnormal_bootstrap_guard_poisons_before_releasing_its_live_slot`;
- `...::prepublication_start_failure_releases_reservation_without_poisoning_listener`;
- `...::unclaimed_server_handoff_preserves_structural_failure_poison`; and
- integration provenance
  `quinn_actor_inbound_and_admission_cleanup_seams_stay_typed_and_single_use`.

After movement, the unit-test prefix changes exactly from
`owned_upstream::quinn_leg_actor::tests::` to
`owned_upstream::quinn_leg_actor::tests::bootstrap_custody::`; the five leaf
names above remain unchanged. The integration-test identity changes from the
bare function name to
`bootstrap::quinn_actor_inbound_and_admission_cleanup_seams_stay_typed_and_single_use`.
P2T records pre- and post-Move exact commands, test counts, and hashes for the
moved function bodies. Any different route, missing test, duplicate test, or
assertion/body change is a stop.

P2A additionally locks the exact argument convergence with
`server_bootstrap_parameter_aggregate_preserves_start_and_failure_identity`.
The tracer observes the same success, prepublication failure, reservation,
endpoint, actor, and cleanup dispositions; it does not inspect copied fields or
bless a new public Interface. A focused code-aware source tracer remains RED
while the allowance stays on `run_server_bootstrap` and rejects it at any child
path. After removal, standalone governance must stop only on the expected 7→6
shrink; standalone and Cargo governance pass only after the Integrator records
that exact lower inventory.

P3–P7 private server-root filters are:

- `pending_ready_command_observer_cancellation_retains_exact_command`;
- `ready_command_full_and_closed_return_the_exact_command`;
- `target_open_pending_retains_exact_authority_until_completion`;
- `cancelled_target_open_observer_reenters_the_same_completion`;
- `ready_session_enqueued_during_cursor_handoff_is_not_lost`;
- `busy_session_cannot_starve_another_ready_session_past_the_derived_bound`;
- `admitted_bootstrap_enters_root_slot_before_first_cancellable_await`;
- `cancelled_bootstrap_observer_reenters_the_same_task_result`;
- `precommit_unwind_retains_exact_bootstrap_quarantine`;
- `accepted_seed_is_fused_into_one_live_whole_root_slot`;
- `failed_current_only_build_returns_exact_seed_and_parent_reservations`;
- `postcommit_unwind_retains_the_same_live_root_in_quarantine`; and
- `custody_only_shutdown_remains_nonzero_with_the_same_private_root`.

Deferred-final filters, unavailable until every downstream ledger gate closes,
are:

- `owner_server_root_public_facade_is_one_private_deep_route`;
- `mini_vpn_upstreamd_is_the_only_non_test_owner_server_root_caller`;
- `whole_server_shutdown_returns_not_joined_while_any_authority_is_nonzero`; and
- `whole_server_shutdown_mints_joined_zero_only_after_every_slot_reclaims`.

Each behavior starts with one causally exact RED, then the minimum GREEN, exact
rerun, bounded repeats, directly affected regressions, provenance, format/diff/
check, standalone and Cargo governance, and independent review. A lexical
source tracer is paired with behavior tests and bounded manual audit; it is not
presented as a general Rust parser.

## 13. Failure tree and acceptance

| Branch | Required outcome | Falsifier |
|---|---|---|
| Success | One admitted bootstrap becomes one exact task/result, one seed, then one whole root in one live slot | Detached bootstrap, escaped `Ok(root)`, duplicate slot, or missing Target/controller/handoff Authority |
| Modeled precommit rejection | Every modeled branch returns/settles the exact stage and reservations; post-split seed failures use `from_parts` | Copied ID/inventory, partial cleanup, published route, or lost seed part |
| Modeled pressure | All fallible capacity/channel/socket/table work precedes commit and returns the exact command/bootstrap/open/seed stage | Unbounded queue, optimistic Opened, post-build allocation, or leaked ready/open lease |
| Send-side custody | Before any send await, one root-owned capability holds the exact command; transfer succeeds once or `Full`/`Closed` returns that command | Observer future owns/drops command, receiver-ready bit substitutes for it, or duplicate transfer |
| Fatal | First fatal fault is retained with exact owning state; unrelated sessions remain serviceable | Raw error escapes, category replaces Authority, global teardown, or retry from unknown disposition |
| Observer cancellation | Dropping `drive_until` loses only a borrow; command/bootstrap/open/ready/root ownership remains re-enterable | Dropped command, detached task, lost completion/wakeup, second admission, or moved-out whole root |
| Arbitrary precommit unwind | The exact precommit aggregate and leases remain in sticky nonzero `BootstrapQuarantined`; no exact-return or clean-zero claim is made without a proven guard | Synthetic seed return, Vacant slot, detached cleanup, retry as Running, or copied quarantine token |
| Postcommit unwind | The already published whole root and first fault remain in sticky nonzero `Quarantined` | Root downgraded to seed, Running restoration, root destruction, detach, or synthetic result |
| P7 custody shutdown | Admission closes and bounded parent stages remain auditable; any live/downstream-unsettled session keeps the same private root nonzero | Session/server zero minted, private root dropped, or listener close used as whole-root evidence |
| Deferred final shutdown | After every downstream gate, bootstraps, opens, sessions, `Z`, slots, sockets, listener, endpoint, and ledgers settle in order | Listener closes before bootstrap disposition or actor zero is treated as server zero |
| Lost wakeup | Clear/recheck protocol observes work published at every interleaving and holds one ready lease | Empty ready set while a session owns runnable control/lifecycle work |
| Deferred false zero | `SessionJoinedZero`/`OwnerServerJoinedZero` require all downstream inventories zero and every slot Vacant; retained/quarantined state returns exact root | P7 custody evidence, preallocated storage, Reclaimable/quarantine, or copied counters accepted as zero |
| Duplicate ownership | At each transition exactly one stage owns bootstrap/open/slot/whole root | `Arc` Authority, Option hole, root task plus table owner, cloned token, or second join path |
| Visibility | External callers can name only crate-root facade values | Public physical root/seed/actor/TargetIo/Quinn/Tokio or a fourth lifecycle entry |
| Deletion | Deleting `server_root` removes owner-server policy; deleting Main removes only composition | Policy survives in listener/Main/callbacks, or adapter deletion destroys domain rules |

Success in one branch does not waive another. In particular, a green route
tracer cannot substitute for Target-open progress, send-side custody, fairness,
or deferred whole-server settlement. Modeled precommit errors, arbitrary
precommit unwind, and postcommit unwind are three different ownership branches;
evidence for one cannot be reused for another.

## 14. Stop rules

Stop and return to architecture on the first occurrence of:

- an unknown or contradictory progress Interface;
- a fourth caller lifecycle entry;
- public or crate-root exposure of physical/session/transport internals;
- a synchronous fake TCP connect, blocking owner thread, or optimistic Opened;
- an unbounded queue, scan, retry, tombstone, bootstrap, open, or ready domain;
- scheduler behavior with no exact send-side command custody, derived starvation
  bound, or lost-wakeup proof;
- moving an Authority-bearing command into an observer-owned send future before
  root custody, or failing to exact-return it on `Full`/`Closed`;
- per-session whole-root task ownership, detach, abort, leak, or Drop-as-cleanup;
- an `Option` ownership hole, cloneable Authority, copied inventory substitute,
  callback/parameter diffusion, dynamic dispatch, macro, or `include!`;
- DATA routed through a full-table lock/scan or per-record allocation;
- a grandfathered parent growing or a new file exceeding its limit;
- an existing warning or `too_many_arguments` allowance moving to a new path,
  or an inventory entry being added to normalize it;
- a child Module written before its parent route, an empty root scaffold, or a
  required `owner.rs`/actor-parent edit outside its net-zero/shrinking phase;
- returning an unpublished `Ok(OwnerPhysicalSessionRoot)` for later slot
  insertion, or adding a callback/generic publisher solely to hide that gap;
- treating modeled precommit error, arbitrary precommit unwind, and postcommit
  unwind as one interchangeable recovery path;
- minting `SessionJoinedZero`/`OwnerServerJoinedZero`, exporting the facade, or
  adding the dedicated binary before all deferred downstream gates close;
- route provenance attempted before P1 architecture acceptance;
- a repair requiring a second attempt after an unexpected red; or
- any attempt to close Case C, R7, Task 4/6, public TUN, VPS, WAN, throughput,
  or M2 without its separate accepted evidence gate.

## 15. Readiness and next gate

Case C implementation readiness remains **4/10**. The CurrentOnly constructor
and living-root direct join are necessary-only evidence; the process route,
bootstrap custody, real TCP Target progress, cross-session scheduler, fused
adoption, non-test caller, and whole-server settlement are absent.

Route/privacy quality remains **9/10**: placement, visibility chain, dependency
direction, and deletion boundary are frozen, and independent plan review has no
open P1 finding. The implemented route reaches 10/10 only after code-aware
provenance proves the actual compiled facade/caller route without Interface
diffusion.

The next smallest gate is one architecture-only
**Target-open + ready-scheduler progress Seam** amendment with Rust zero. It is
not a Rust Move, route scaffold, provenance RED, or implementation authorization.

## 16. Source links

- [Current status](../current/STATUS.md)
- [Product rules](../../Rules.md)
- [Project domain language](../../CONTEXT.md)
- [ADR-0015](../adr/0015-path-diverse-resumable-upstream.md)
- [Accepted Case C architecture](2026-09-04-knife16-r7-case-c-owner-server-root-architecture.md)
- [Accepted direct-join architecture](2026-09-03-knife16-r7-current-only-direct-join-amendment.md)
- [Direct-join local results](2026-09-03-knife16-r7-current-only-direct-join-local-results.md)
- [`src/lib.rs`](../../src/lib.rs)
- [`src/main.rs`](../../src/main.rs)
- [`owned_upstream/mod.rs`](../../src/owned_upstream/mod.rs)
- [`quinn_leg_composition.rs`](../../src/owned_upstream/quinn_leg_composition.rs)
- [`owner.rs`](../../src/owned_upstream/quinn_leg_composition/owner.rs)
- [`owner_initial.rs`](../../src/owned_upstream/quinn_leg_composition/owner_initial.rs)
- [`physical_session_root.rs`](../../src/owned_upstream/quinn_leg_composition/owner/physical_session_root.rs)
- [`quinn_leg_actor.rs`](../../src/owned_upstream/quinn_leg_actor.rs)
- [`session.rs`](../../src/owned_upstream/session.rs)
- [`owner_target.rs`](../../src/owned_upstream/owner_target.rs)
- [`target.rs`](../../src/owned_upstream/target.rs)
- [Code-growth governance](../../tests/code_growth_governance.rs)

This file is **ACCEPTED / PLAN-ONLY / RUST NOT AUTHORIZED / PREREQUISITES
OPEN**. Acceptance freezes this conditional plan only; P1 architecture work,
Rust implementation, Case C completion, public routing, and external gates each
still require their own authorization and accepted evidence.

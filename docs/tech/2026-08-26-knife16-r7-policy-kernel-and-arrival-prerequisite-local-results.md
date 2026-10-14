# Knife16 R7 policy kernel and arrival prerequisite local results

Date: 2026-08-26.

Status: **LOCALLY ACCEPTED AS NECESSARY-ONLY; FULL R7, PUBLIC TUN, WAN, AND
M2 ARE NOT ACCEPTED**.

## Outcome

This stage closes the deterministic local seam that R7 needs before a physical
no-oracle two-leg composition can be tested:

- one endpoint-session monotonic clock is shared by A, B, composition, and the
  client root;
- the Quinn actor captures immutable inbound arrival time after a complete body
  read and inside the same atomic publication reservation;
- semantic timers use actor arrival as deadline evidence while root time is
  used only to decide that a timer is due;
- the first physical hard failure captures one stable same-clock terminal time
  before delayed cleanup or join;
- the owner reducer exposes a typed observation only after a positive committed
  application ACK releases nonzero replay bytes; and
- a bounded policy kernel covers owner replay/ACK progress, exact hint retry,
  client rejection, monotonic hint sequences, and one switch epoch/nonce under
  all hard/local/hint trigger orderings.

This is not full R7a-d reachability. The policy kernel still accepts trusted
typed inputs, and the production root does not yet compose the physical B
actor, owner hint producer, client hint adapter, and single trigger arbiter.

## Accepted invariants

### Same-clock actor arrival

`SessionMonotonicClock` owns one fixed Tokio epoch and process-local source
identity. Checked conversion rejects time before the epoch and nanosecond
overflow. A replacement actor whose clock source differs from the current
composition is rejected before standby or controller mutation.

After a complete record body is read,
`InboundPublicationTracker::reserve_arrival` captures checked session time and
reserves the publication epoch under one lock. The exact `arrived_at` then moves
through `InboundRecord`, `ControllerInboundRecord`, blocked retry, the typed
actor envelope, composition, controller, and reducer classification. Retry and
root scheduling never recapture it.

Before-deadline and exact-deadline input wins even when root service is delayed.
Post-deadline input cannot extend the absolute timer. A reserved but not yet
published arrival returns the original timer as publication-pending; queue
ordering remains governed by the publication epoch.

### Stable hard-terminal time and capacity

The first `ActorFirstOutcome::Failed` transition captures the only canonical
`Option<SimTime>`. One private non-cloneable `ActorTerminalStampPair` mints the
handoff and root-outcome handles needed during cleanup overlap. Join verifies
that both handles belong to the same actor, discards one, and moves the survivor
through joined/composition state. Rejection restores the original witness.

Actor admission checks the exact timestamp-storage formula from the R7 plan:

```text
(inbound_messages + 4) * size_of::<SimTime>()
  + size_of::<Option<SimTime>>()
  + 2 * size_of::<ActorTerminalStampWitness>()
```

All arithmetic is checked before actor publication.

### Reducer-derived ACK observation

`SessionSupervisor` creates an opaque ACK observation only after the matching
reducer receipt commits a positive advancement and releases nonzero replay
bytes. `OwnerTargetExecutor` is the sole private-gate consumer. Zero advancement,
duplicates, reducer rejection, and decoded or transport-only ACK evidence do
not create progress. Final positive application ACK is covered and releases
the remaining target-read ownership.

### Bounded policy kernel

The local kernel checkpoint originally retained at most two directional
progress entries per live flow, with a necessary-only `2 * max_flows`
manifest. It keeps only the current oldest outstanding offset and latest
positive application ACK point; it stores no ACK history.

**2026-09-01 capacity correction:** that checkpoint bound is not sound for
physical production composition. A live-flow slot can be reused while the old
flow's progress remains through terminal grace, so the production Authority and
current R7 architecture require checked
`2 * (max_live_flows + max_terminal_tombstones)`. The 2026-08-26 policy
behavior evidence remains accepted as necessary-only, but its scan-capacity
manifest is not promotion evidence. The correction plan and falsifiable cases
are recorded in
`docs/tech/2026-09-01-knife16-r7-physical-root-move-and-architecture-convergence-local-results.md`.

Owner hint preparation rejects idle/no-replay, wrong generation/direction/
offset, terminal flow, stale B, and non-due ACK age. Queue pressure retains the
same pending hint and sequence. Client validation rejects wrong or stale
correlation without changing its snapshot, while a lower hinted replay offset
remains valid when the local accepted frontier is ahead after ACK loss.

Hard failure, local stall, and authenticated hint enter one local arbiter. All
six orderings reuse one switch epoch and one attach nonce and commit at most one
next generation. A later hard failure upgrades the existing soft transaction
rather than minting another attach.

## Deterministic evidence

Focused tracer bullets cover:

- checked shared-clock before/equal/after conversion and fail-closed overflow;
- complete-read atomic arrival reservation, blocked retry, in-flight
  publication, different-clock rejection, and exact deadline arbitration;
- a real queued B ACK that arrived before its deadline but was serviced by root
  after the deadline, repeated ten times;
- first-fatal timestamp stability, probe-deadline handoff through delayed join,
  and local shutdown without a false hard-terminal timestamp;
- positive, duplicate, rejected, and final owner application ACK behavior;
- owner hint mint/retry/progress capacity and client rejection/sequence rules;
  and
- every hard/local/hint ordering plus a 100-iteration soft-to-hard race.

## Local validation

| Gate | Result |
|---|---:|
| Session-clock focused | `2/2` |
| Reverse-switch policy kernel | `8/8` |
| R7-name focused filter | `13/13` |
| OwnerTarget R7 ACK | `2/2` |
| Quinn client root | `16/16` |
| Complete Quinn actor/composition Module | `117/117` |
| Delayed-root real B ACK repeat | `10/10` |
| Default library | `1309 passed; 3 ignored` |
| All targets/all features | library `1323 passed; 3 ignored`; main `2/2`; concurrency `10/10; 4 ignored`; Knife16 provenance `145/145`; protocol `25/25`; session API `1/1`; repository-layout provenance `6/6` |
| Dev/release all-target/all-feature checks and release build | PASS |
| Current six-class production-library Clippy diagnostic | PASS |
| Rustdoc | PASS with the established `15` unrelated warnings |
| Vendored Quinn with exact local proto | `47 passed; 3 ignored`; integration `1 ignored`; doctest `1/1` |
| Vendored quinn-proto | `330/330`; doctests `3/3` |
| Independent final review | unresolved P0/P1/P2 and test-layout violations `0/0/0/0` |

The six-class Clippy command used `-D warnings` plus the disclosed current
baseline allowances `too_many_arguments`, `enum_variant_names`, `manual_clamp`,
`collapsible_if`, `result_large_err`, and `large_enum_variant`. It is diagnostic
evidence, not a claim that the historical four-class gate was reproduced. No
source-level lint allowance was added.

All new unit-test bodies follow the project layout rule under private
`src/<module>/tests/` children. Root `tests/` contains only integration,
public-contract, and repository-provenance tests. The shrink-only repository
ratchet passes `6/6`.

Core accepted source SHA-256 values:

```text
f29d1eec46521eaf5fbc62fe9a2754ab9a9be9ff6877d8fe92e1fe71419fe90b  src/owned_upstream/session_clock.rs
11eed50fb099461803b380ab147460c96b90ecb4dabc3f0525c1bc21e356cf4c  src/owned_upstream/reverse_switch.rs
5ee04b4263439fa6325fd772f6246b2594757e2f6715b0f1ea8503cbef3ad009  src/owned_upstream/quinn_leg_actor.rs
bd5de09eea924cf1f87fbb0adc6760d87f9a7cf50a9f2cf22eaa6ff62ce2f3b4  src/owned_upstream/quinn_leg_composition.rs
5021c743340ea2bd5383f62745c1ff99923e19c3c4c50358ef9cdfadd6fbdc2f  src/owned_upstream/quinn_client_root.rs
01d82c33524b326a561d2c8a170a43a08b291c2cd7ebde1159dca92a1f8bfc82  src/owned_upstream/supervisor.rs
5b4fbf861f577bb838713e11c1fc306687a6bfe32df4fd4329bb617e13deb2de  src/owned_upstream/owner_target.rs
```

## Rejected evidence repaired during the stage

- Several older real-actor deadline tests supplied artificial root observation
  time or assumed `SimTime::ZERO`. Once actor arrival became authoritative,
  those fixtures no longer proved the intended side of the deadline. They now
  derive deadlines from the shared session clock and explicitly wait across the
  boundary.
- Three source-provenance assertions retained old signatures after arrival and
  terminal-stamp ownership were intentionally strengthened. They were updated
  to require the new timestamp field, shared-clock bootstrap, terminal-stamp
  barrier, and arrival-preserving adapter. The final target passes `145/145`.

## Production blockers

The following remain open and prevent a full R7 or M2 claim:

- `QuinnClientRoot` still discards `ClientInboundObservation` and has no B,
  hint, ACK-stall timer, or switch-arbiter scheduling category;
- the OwnerTarget observation API has no production owner-controller consumer;
- supervisor observations do not yet export all outstanding-replay and terminal
  facts required by the owner hint producer;
- physical B construction, bidirectional probe response, authenticated hint
  transport, and client physical-provenance adaptation are not composed;
- existing hard-failure/local-stall paths do not yet enter one production
  arbiter; and
- the public TUN entry still selects `LegacyOwnedUpstream`.

Therefore R7a-d no-oracle physical acceptance, Task 4, R8-R9, UDP two-leg work,
owner/server composition, WAN, throughput, and the M2 one-second uninterrupted
acceptance remain open. No macOS TUN, VPS, WAN, throughput, M2, M3, user
traffic, commit, or push is claimed by this result.

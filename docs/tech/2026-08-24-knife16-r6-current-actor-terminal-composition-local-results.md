# Knife16 R6 Current-Actor Terminal Composition Local Results

Date: 2026-08-24

Status: **NECESSARY-ONLY LOCAL PASS; TASK 4 AND M2 NOT ACCEPTED**

## Outcome

The exact spontaneous terminal from the production-shared Quinn current actor
now reaches the matching client-controller slot before the actor's physical
connection/Endpoint/socket close tail finishes. The controller consumes at
most one existing `LegLost` capability, emits the exact
`ResumeGraceStarted { generation }` effect, retains terminal-bound cleanup
authority, and makes every re-drive inert or repeat the same sticky failure.

On a structurally healthy terminal handoff, this closes the concrete logical-
lifecycle block in which a real peer close or fatal child could be visible to
the actor while the controller remained Active. If the reporter cannot mint
the terminal, the composition instead repeats `TerminalHandoffClosed` without
calling or mutating the controller. This stage does not yet attach B after
loss, replay through B, observe the first positive B application ACK, detect a
silent blackout, or close the complete checked recovery budget. It therefore
does not solve or run the M2 one-second acceptance.

No TUN, VPS, WAN, throughput, M2, M3, user traffic, or external resource was
used.

## Closed ownership transaction

The actor root now publishes one non-cloneable `ExactLegTerminal` only after
all fixed children have stopped and its inbound, pending delivery, message,
byte, and DELIVERED ownership is zero. Publication occurs before the existing
physical close tail. The started actor remains the sole owner of the later
structural join and exact physical-incarnation witness.

The composition transaction is split into explicit phases:

```text
actor terminal handoff
  -> exact actor/controller incarnation check
  -> controller bind_current_terminal
  -> discard exact recovery/tail ownership
  -> consume one PendingClientLegLoss
  -> Applied or sticky ConsumedFailed

in parallel afterward:
  actor connection/Endpoint/driver/socket cleanup
  -> exact zero-ownership Joined witness
```

The controller now separates two capabilities that were previously
conflated:

- `PendingClientLegLoss` may emit the one logical loss event; and
- `TerminalBoundRetiredClientLeg` retains the exact terminal as future
  post-loss retired cleanup authority.

This prevents the logical transition from destroying the terminal that a
later B install and retired-A structural clear will need.

## Fatal, policy, and shutdown linearization

Actor fatal publication and every current-controller policy commit share the
same `ActorFirstFailure` mutex. The transaction holds that mutex for the whole
synchronous controller operation, not only for a preliminary check.

- If the fatal wins first, `open_local`, recovery drive, standby registration,
  replacement, and Unbound shutdown execute no controller closure, request
  cooperative actor cancellation, and return the same root failure.
- If a controller operation wins first, a child fatal cannot publish in the
  middle of that commit.
- If Unbound shutdown publishes `ShuttingDown` first, a later local-cancel
  terminal is cleanup-only and cannot synthesize `LegLost`.
- If fatal publication wins first, shutdown leaves the composition Running so
  the exact terminal drive can apply the one loss transaction.

No actor raw bytes, queue, `LegIo`, permit, or task owner crosses this
payload-free policy gate.

## Error and retry semantics

Before controller bind, a rejected transaction returns the same terminal to
the actor. After bind, the terminal never rolls back across layers.

- `RecoveryCleanupRejected` occurs before the loss capability is consumed, so
  the composition remains `ControllerBound` and retries only the cleanup
  phase.
- `AlreadyDiscarded`, `SupervisorRejected`, and `EffectInvariant` occur at or
  after the one-shot loss boundary. They become `ConsumedFailed`, repeat the
  same error on every drive, and remain sticky through physical join.
- Only exact generation, reason, and effect agreement becomes `Applied`.
- Physical join may retain an exact `0/0` joined witness after a sticky logical
  failure, but it cannot rewrite that failure as success.

## Deterministic TDD and review repairs

The first real peer-close RED published an actor fatal while `open_local`
could still mutate the controller before terminal handoff. The shared
first-failure policy transaction closed that race. The accepted tracer proves
that fatal publication rejects controller admission immediately, while the
logical loss remains pending until the exact terminal is available.

The first post-consumption classification treated controller discard failures
as healthy completion. A controller-only fault tracer proved that
`AlreadyDiscarded` could otherwise become a false `Applied`. The sticky
`ConsumedFailed` state now preserves the same drive and join error without
replaying the loss.

Additional tracers prove:

- a pre-bind rejection restores the same terminal and the next production
  drive applies it exactly once;
- fatal-first and shutdown-first obey the shared-mutex winner, while a
  deterministic in-closure gate proves the first-failure mutex remains held
  across the complete synchronous policy commit;
- a Recovering current B loses exactly once while retired A's controller route,
  physical actor, and peer remain unchanged;
- both real close paths apply the exact terminal-reason mapping of the
  observed first-failure `Arc`, including across pre-bind rollback;
- retired A can clear independently after B loss;
- the B actor's early terminal is observable while its physical close tail is
  deliberately blocked;
- a real reporter transition failure closes the terminal handoff before the
  composition callback, then repeated drive leaves controller snapshot,
  routes, and `LegIo` counters unchanged; and
- final B structural join still returns exact zero message/byte ownership.

The architecture spec uses the implementation state name `Applied` for the
conceptual “current is logically lost” state. The final review's missing
composition-level handoff proof is now locked by the real reporter-failure
tracer rather than inferred from actor-root tests.

## Accepted local validation

- fixed Quinn actor module: `54/54`;
- focused client controller: `11/11`;
- actual peer-close terminal transaction: `100/100` on one frozen binary;
- Recovering-B loss with retired-A isolation: `100/100` on the same frozen
  binary;
- reporter-failure/closed-handoff controller isolation: `100/100` on that same
  frozen binary;
- all targets/all features: library `1165 passed; 3 ignored`, main `2/2`,
  concurrency `10/10; 4 ignored`, typed provenance `130/130`, protocol
  `25/25`, public session API `1/1`;
- four explicitly ignored concurrency sweeps: `4/4`;
- real-smoltcp adapter: `2/2`, with its frozen-binary core tracer `100/100`;
- development and release all-target/all-feature checks plus release build:
  PASS;
- strict production-library Clippy with only the documented baseline
  `too_many_arguments`, `enum_variant_names`, `manual_clamp`, and
  `collapsible_if` allowances: PASS;
- rustdoc: PASS with the existing `15` unrelated warnings;
- isolated vendored Quinn with `cargo tree` proving the exact absolute local
  quinn-proto `0.11.16`: `47 passed; 3 ignored`, one integration test ignored,
  doctest `1/1`;
- isolated vendored quinn-proto: `330/330`, doctests `3/3`;
- root rustfmt, `git diff --check`, typed source provenance, generated-artifact,
  and focused tracked/untracked secret-shape gates: PASS.

The three repeated Quinn tracers used frozen test binary SHA-256
`3a01b611d14d2d2839e54c35b9f22bd8b2e8ad91adf540c6221e91c32ec9ced6`.
Accepted source SHA-256 values are:

```text
9b7ff831a752145fe514586a764bd2a13c418a31405dc56b8b543e1976c7df73  src/owned_upstream/quinn_leg_actor.rs
1b4b1d4f39b459d1303100665c4fd462b023175b01457ca66de7b8ca15cd059f  src/owned_upstream/quinn_leg_composition.rs
399c31d2aca6f81a0ffe183d5fd602a75b3ac5936b36eac7bc35a58cad9b7ef7  src/owned_upstream/client_controller.rs
3cd28682ac37f19b21dbaae47a81f08c73cf552c9f69dff00e5b540936c55a60  src/owned_upstream/leg.rs
48aff42c15d81356002246fdb26e89a5c954603737b41238ba1a13d995122e24  tests/knife16_owned_leg_provenance.rs
```

Two exploratory strict-Clippy invocations used the wrong historical allowance
sets and were rejected as command evidence. The accepted command above uses
the repository's current four production-library baseline classes and reports
no new mini_vpn lint.

Independent lifecycle, controller-transaction, and architecture reviews found
no code-level P0/P1. Review findings for `CurrentLost` versus `Applied`, the
missing composition-level closed-handoff tracer, the in-closure mutex witness,
and exact first-failure reason correlation are all repaired. Final unresolved
P0/P1/P2 is `0/0/0`.

## Remaining boundary and next step

This is a necessary-only sub-slice of R6a, not complete R6a acceptance. The
real TUN entry still selects `LegacyOwnedUpstream`, and no production session
nursery/root continuously drives this composition.

The next bounded stage must complete the post-loss transaction: install the
already registered healthy B after actual A loss, replay through B, require the
first exact positive B application ACK, move A's terminal-bound cleanup
authority into the retired slot, and clear it only with A's exact structural
joined witness. It must then add R6b silent-blackout detection using an exact
outstanding probe epoch and fresh B health, arbitrate close/probe/deadline
races, and close the checked aggregate `<=500ms` recovery-capacity sum.

That work still does not cover R7 reverse-only authenticated hints, R8 fault
matrix, R9 real-smoltcp replacement parity, UDP two-leg delivery, a production
owner/root, local `>170 Mbit/s`, WAN, or M2. Do not run macOS TUN, VPS, WAN,
throughput, or M2 yet.

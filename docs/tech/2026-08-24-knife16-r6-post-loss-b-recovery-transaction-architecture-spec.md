# Knife16 R6 Post-Loss B Recovery Transaction Architecture

Date: 2026-08-24

Status: **ACCEPTED LOCALLY AS A NECESSARY-ONLY R6A SLICE; TASK 4 AND M2 NOT
ACCEPTED**

## Stage goal

Close the remaining actual-close transaction after the accepted current-actor
terminal slice:

```text
exact A terminal applied
  -> client reducer Legless, A terminal retained by the controller
  -> already authenticated and registered B receives exact next ATTACH_ACCEPTED
  -> one controller/composition commit installs B and retires A
  -> bounded replay enters only B's sole FIFO
  -> first positive B application ACK intersects an installed replay range
  -> exact joined A clears only the terminal-bound retired-A slot
```

The transaction must retain the existing Target/flow/replay model. It must not
reopen a Target socket, synthesize a second `LegLost`, mint a second terminal,
or let a failed B install consume either actor.

## Non-goals

- silent-blackout detection, probe epochs, PTO/deadline expiry, or B freshness;
- the complete checked `500ms` recovery budget;
- reverse-only authenticated hints and all R7 work;
- production session-nursery/root reachability from the real TUN entry;
- UDP two-leg switching, WAN, throughput, VPS, macOS TUN, or M2 acceptance.

This is a necessary-only R6a correctness slice. It cannot by itself prove that
the one-second continuity test will pass.

## Ranked, falsifiable failure hypotheses

1. The composition has no post-loss entry, so valid B ownership is rejected by
   the normal Running/Unbound policy gate after A becomes `Applied`.
2. The controller incorrectly reuses its Active-only normal-begin gate even
   though the reducer accepts an exact next-generation replacement from
   `Legless`.
3. B can commit without atomically moving `RecoveryDiscarded` A into the sole
   retired slot, losing its terminal-bound cleanup authority or creating two
   physical owners for one controller slot.
4. Resetting the composition terminal state before the controller commit can
   admit current operations into a half-committed layout or allow a second
   `LegLost`.
5. Retired clear still requires a second actor terminal. A whose terminal was
   already consumed by the logical-loss transaction will join with an exact
   physical incarnation and zero ownership but no remaining terminal payload.

The controller tracer distinguishes hypotheses 2 and 3. The real Quinn tracer
distinguishes 1, 4, and 5 and proves the complete ordering through the first
positive B ACK.

## Existing seam and required deepening

The reducer and controller already own the hard pieces:

- `TerminatingCurrentClientTransport::RecoveryDiscarded` retains exact A
  authority, terminal, transport, generation, reason, flows, and replay;
- `PostLossClientRetirementSource` expresses the one-way move from that
  phase into the retired slot;
- `publish_replacement_recovery` installs one current transport, a bounded
  pre-encoded recovery cursor, and the replay-range-bound application-ACK
  witness;
- `try_receive_current` recognizes recovery only when a positive ACK advances
  a range actually owned by the installed replacement;
- the Quinn composition already owns exactly one current actor and at most one
  retired actor and can structurally join/clear the latter.

The missing interface is a branch-by-abstraction deepening, not a new reducer:

1. add a controller post-loss begin that consumes ordinary
   `AcceptedClientRegisteredAttach` while and only while the exact current is
   `RecoveryDiscarded`/Legless;
2. make the accepted-attach install helper carry the controller retirement
   source through every synchronous rejection;
3. add a Quinn post-loss begin that validates the terminal-applied current
   physical incarnation, commits the controller, then infallibly mirrors
   A-current/B-candidate into A-retired/B-current and resets only B's terminal
   state to `Unbound`;
4. expose the production-shared composition seam that mints the existing
   `ResumableTcpPortFactory` and consumes typed `ClientControllerDriverInput`,
   so the real Quinn tracer creates unacknowledged DATA through the same
   source/reducer path a future session root must drive;
5. make joined retired clear accept exactly one of two terminal shapes:
   unbound retired A plus `Some(terminal)`, or terminal-bound retired A plus
   `None`.

No raw `LegIo`, queue, attached leg, terminal, or actor escapes the existing
controller/composition boundary.

## Transaction and ownership invariants

1. Entry requires composition `Running`, current-terminal `Applied`, no prior
   retired actor, no joined-current witness, exact A physical/controller
   identity, and a live exact B actor containing the accepted B transport.
2. Before B's reducer commit, every rejection returns B acceptance, `LegIo`,
   sole FIFO, and actor; A remains `RecoveryDiscarded` and current-terminal
   remains `Applied`.
3. The reducer commit, A retirement-source move, B current publication,
   recovery reservation, and pending first-ACK replay ranges are one
   synchronous controller transaction.
4. After controller success, the composition performs only infallible owner
   moves under its exclusive `&mut self`: A current -> retired, B candidate ->
   current, and terminal state -> `Unbound` for B.
5. A failed or cancelled actor cannot be installed. B's actor transaction
   holds its first-failure mutex across extraction and controller commit.
6. The recovery barrier blocks B inbound before decode until every prepared
   replay/control effect has been admitted and its delivery tail permits
   normal receive.
7. Only a positive application ACK for B's exact session, generation, flow,
   direction, and installed replay intersection can publish
   `ReplacementRecovered`. QUIC delivery alone is insufficient.
8. A remains the only retired actor and cannot mint DATA/ACK/session events.
   B remains current while A joins.
9. A joined witness may become ready in parallel with B recovery, but a
   terminal-bound retired slot with a pending exact replacement-ACK range is
   retained until that range receives its first positive application ACK.
10. Normal retired A clears with its one joined terminal. Terminal-bound A
   clears only when its same physical actor joins with zero ownership and no
   second terminal payload. Missing/unexpected terminal shapes fail closed and
   restore the joined payload.
11. Cleanup of A never emits `LegLost`, changes B generation, releases B
    replay, closes Target state, or interrupts B exchange.
12. A replacement with no DATA replay has no pending first-ACK witness and may
    clear A without waiting for an impossible ACK. It must not emit
    `ReplacementRecovered`; the acceptance tracer deliberately owns non-empty
    client-to-Target replay.

## Capacity and reachability gate

The target design's retained-coverage math remains:

| Rate | Bytes per second | 500ms per direction | Full duplex |
|---|---:|---:|---:|
| 100 Mbit/s | 12,500,000 | 6,250,000 | 12,500,000 |
| 170 Mbit/s | 21,250,000 | 10,625,000 | 21,250,000 |
| 240 Mbit/s | 30,000,000 | 15,000,000 | 30,000,000 |

At 100 Mbit/s, one 5ms quantum is 62,500 bytes and 100 quanta are exactly
6,250,000 bytes per direction. This stage does not enlarge or claim those
capacities. Its real-loopback tracer intentionally uses the existing small
test limits and proves ownership/order only. Encoded 500ms coverage requires
96/163/229 maximum-size DATA frames and 6,253,552/10,631,031/15,008,473 bytes
at 100/170/240 Mbit/s respectively. The current fixture's 16-message/1MiB
`LegIo` lifetime is therefore explicitly not capacity evidence.

The code-level path made reachable by this stage is:

```text
registered B actor/ATTACH_ACCEPTED
  -> QuinnClientSessionComposition post-loss begin
  -> ClientSessionController post-loss begin
  -> SessionModel prepared replacement
  -> PreparedClientRecovery / B LegOutboundQueue
  -> composition drive_recovery_one + try_flush_current
  -> Quinn actor DATA writer / QUIC stream
  -> server-B actor / existing OwnerTargetExecutor / MemoryTarget positive accept
  -> owner reducer mints exact B application ACK / server-B FIFO / QUIC
  -> client-B Quinn inbound
  -> controller observed apply / replay-range release witness
  -> retired A actor join / exact controller clear
```

Old paths that remain active or unreachable:

- the real TUN entry still selects `LegacyOwnedUpstream`;
- there is no production session root continuously driving attach, recovery,
  actor flush/receive, Target work, ACK, and cleanup;
- silent failure, B health, the checked recovery-budget sum, R7 reverse hints,
  UDP switching, and WAN ingress diversity remain absent.

Therefore this change is necessary for R6a but not sufficient for 100/170
Mbit/s, `<1s`, Task 4, or M2.

Failure discriminators in the tracer are exact controller phase/generation,
current/retired routes and actor incarnations, replay effect count, actor
message/byte counters, ordered-delivery receipts, the
`ReplacementRecovered` observation, replay-owned bytes, and joined A's
terminal/zero-ownership shape.

## TDD sequence

1. Controller RED: after exact A terminal/discard, a normal registered B
   acceptance cannot enter the missing post-loss transaction. GREEN installs
   B, moves terminal-bound A once, preserves replay, and restores all inputs on
   wrong transport/prepare rejection.
2. Real Quinn RED: register B, apply a real A peer-close while A's physical
   close tail is blocked, obtain B `ATTACH_ACCEPTED`, and call the missing
   composition transaction. GREEN atomically installs B without reopening the
   existing Target/flow state.
3. Drive the real B recovery FIFO under the existing barrier, flush its bytes,
   and pass replay DATA through server B into the existing
   `OwnerTargetExecutor`/`MemoryTarget`. Only the owner reducer's resulting
   exact positive application ACK, returned through server B's FIFO and Quinn,
   may produce `ReplacementRecovered` and release the intersecting replay
   range. A hand-built ACK or direct reducer event is forbidden evidence.
4. Join terminal-bound A while B remains live. The joined actor must carry zero
   messages/bytes and no second terminal. If the witness arrives before the
   B ACK, clear remains retryably blocked; after the ACK, clearing A must leave
   B generation, route, snapshot, and exchange intact. A physical join is not
   part of the continuity clock, which ends at the first B application ACK.
5. Add focused rejection/race cases for wrong phase, wrong actor identity,
   candidate actor failure, install rejection, duplicate entry, unexpected
   joined-terminal shape, and cleanup retry ownership.
6. Restore actor/controller/standby/LegIo/supervisor/provenance/concurrency/
   real-smoltcp, all-target, release, Clippy, rustdoc, vendored Quinn/proto,
   formatting, diff, and secret gates before local acceptance.

## Stop rules

- Stop if B installation needs a synthetic/copy terminal or a second
  `LegLost`.
- Stop if any rejection loses A or B actor, accepted attach, transport, queue,
  recovery reservation, replay range, or Target ownership.
- Stop if A physical join is placed on the logical B-install or first-ACK
  critical path.
- Stop if QUIC delivery, queue admission, or timer expiry is treated as an
  application ACK.
- Stop if reaching the next Mbps or M2 gate would still require the legacy
  TUN path; report this slice as necessary-only and continue architecture
  composition instead of running WAN acceptance.

## Local acceptance disposition

The implementation and focused real-Quinn transaction now pass. The accepted
transaction starts only after A's exact loss is applied, installs an ordinary
already-registered B, replays non-empty DATA through B into the existing
Target, accepts recovery only after the owner reducer returns the first
positive application ACK, releases the exact replay range, and clears only
terminal-bound joined A.

Final review also made shutdown and rollback part of the same ownership proof:

- the typed driver input is opaque and can be minted/unwrapped/re-wrapped only
  by the supervisor reducer gate;
- pending-ACK retired A and current B remain composition-owned across caller
  cancellation and can be joined by a later retry without forging recovery or
  releasing replay;
- absence of a Tokio runtime is detected before phase mutation, cancellation,
  or taking either actor;
- wrong retired identity fails before cancellation; and
- one private helper is the sole retired-join task executor.

The complete result and final gate evidence are recorded in
`docs/tech/2026-08-24-knife16-r6-post-loss-b-recovery-transaction-local-results.md`.
The next local stage is R6b silent-probe failure plus close/probe/deadline
arbitration, followed by R6d checked `<=500ms` capacity and production-root
closure. No TUN, VPS, WAN, throughput, or M2 run is authorized by this result.

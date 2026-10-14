# Knife16 R6 Post-Loss B Recovery Transaction Local Results

Date: 2026-08-24

Status: **ACCEPTED LOCALLY AS A NECESSARY-ONLY R6A SLICE; TASK 4, WAN,
THROUGHPUT, AND M2 NOT ACCEPTED**

## Outcome

The exact actual-close recovery transaction is locally closed:

```text
real peer-close on A
  -> exact A terminal applied once / client becomes Legless
  -> ordinary already-authenticated and registered B receives ATTACH_ACCEPTED
  -> one post-loss controller/composition commit installs B and retires A
  -> non-empty DATA replay enters only B's bounded FIFO
  -> server B delivers DATA to the existing OwnerTargetExecutor/MemoryTarget
  -> exact positive Target acceptance lets the owner reducer mint B's ACK
  -> client B observes ReplacementRecovered and releases the replay range
  -> exact terminal-bound joined A clears without disturbing current B/Target
```

The main real localhost Quinn tracer owns nine replay bytes. Its ordering makes
actual A loss precede B acceptance, and it forbids QUIC delivery, queue
admission, a hand-built ACK, or timer expiry from serving as recovery proof.
The existing Target accepts the replay before the existing owner reducer mints
the first positive application ACK. Only that ACK advances
`ReplacementRecovered` from `0` to `9` and releases the exact replay bytes.

No second `LegLost`, Target reopen, duplicate terminal, replay forgery, or
active-B disturbance is required.

## Production-shared changes

- `ClientSessionController` has an exact post-loss begin that consumes the
  retained `RecoveryDiscarded` A source only from the matching Legless state.
  Synchronous rejection restores the ordinary accepted B attach, `LegIo`, sole
  FIFO, actor, and A authority.
- The standby install helper is generic over the controller retirement source,
  so normal and post-loss installation use the same reducer transaction and
  exact-return path.
- `QuinnClientSessionComposition` validates the terminal-applied A physical
  incarnation, commits B through the controller, and then performs only
  infallible exclusive owner moves: A current to retired, B candidate to
  current, and B terminal state to `Unbound`.
- A typed driver-input transaction now carries reducer pressure back with the
  exact original input. The input wrapper is opaque; only the supervisor's
  reducer gate can mint, unwrap, or re-wrap it.
- Terminal-bound retired A is application-ACK gated when replay is non-empty.
  Zero-replay recovery clears without waiting for an impossible ACK and does
  not emit `ReplacementRecovered`.

## Shutdown and lifecycle closure

Shutdown no longer deadlocks behind a pending first recovery ACK. It may
physically join pending-ACK retired A and current B without consuming the
controller recovery witness, publishing a false recovery event, or releasing
replay. The unique join task remains stored in the composition when an outer
shutdown waiter is cancelled, and a later call resumes the same cleanup.

Before any irreversible phase change, cancellation, or actor take, shutdown
validates the exact current/retired layout and confirms a Tokio runtime is
available. Wrong retired identity and missing-runtime branches therefore
preserve the same snapshot, routes, actors, and replay. One private
`spawn_retired_join` helper is the sole task executor, which keeps task
provenance mechanically checkable.

## Capacity and reachability classification

This is an ownership/order proof, not a capacity result. The retained-coverage
requirement remains 6,250,000 bytes per direction for 500ms at 100 Mbit/s,
10,625,000 bytes at 170 Mbit/s, and 15,000,000 bytes at 240 Mbit/s. Including
the current frame bounds requires 96/163/229 maximum-size DATA frames and
6,253,552/10,631,031/15,008,473 bytes respectively. The small real-loopback
fixture is deliberately insufficient evidence for those values.

The real TUN entry still selects `LegacyOwnedUpstream`, and no production
session nursery/root continuously drives attach, recovery, Target work, ACK,
and cleanup. Silent failure, exact-probe health, close/probe/deadline
arbitration, checked `<=500ms` recovery capacity, R7-R9, UDP two-leg switching,
WAN path diversity, throughput, and the M2 one-second acceptance remain open.

## Final local gates

The final all-features libtest binary was
`target/debug/deps/mini_vpn-802b66297ce9f029`, SHA-256
`0b72a7fb3992f82af9198d789d0468f4ddd5a9c094fc64e8f3f4743391f9324f`.

| Gate | Result |
|---|---|
| `cargo test --all-targets --all-features` | lib `1172 passed; 3 ignored`; main `2/2`; concurrency `10/10` plus `4 ignored`; provenance `130/130`; protocol `25/25`; session API `1/1` |
| Fixed Quinn actor module | `58/58` |
| Ignored concurrency sweeps | `4/4` |
| Real-smoltcp fake adapter | `2/2` |
| Main post-loss real-Quinn transaction | `100/100` on the frozen binary |
| Pending-ACK shutdown/cancel/retry tracer | `100/100` on the frozen binary |
| Real-smoltcp core | `100/100` on the frozen binary |
| Zero-replay clear | `100/100` on the frozen binary |
| Candidate-B fatal-first rollback | `100/100` on the frozen binary |
| Input commit-first/fatal-first linearization | `100/100` on the frozen binary |
| Typed driver pressure exact-return/retry | `100/100` on the frozen binary |
| Strict production-library Clippy | pass with the repository's four established allowances |
| `cargo check --all-targets --all-features` | pass |
| `cargo check --release --all-targets --all-features` | pass |
| `cargo build --release` | pass |
| `cargo doc --no-deps --all-features` | pass with 15 pre-existing unrelated missing-doc warnings |
| Vendored Quinn / quinn-proto isolated offline gates | Quinn `47 passed; 3 ignored`, doctest `1/1`; proto `330/330`, docs `3/3` |

Formatting, diff integrity, generated-vendor-artifact absence, typed
provenance, and changed/untracked secret-shape checks pass. Independent
controller and composition reviews report unresolved P0/P1 `0/0`.

## Rejected false proofs during the stage

- A focused command using a leaf name with `--exact` selected zero tests; it
  was rejected and rerun with a non-zero match.
- A repeat wrapper accidentally selected the product binary rather than the
  libtest binary; its CLI panic was rejected and no network/TUN action ran.
- A retired-child gate armed after the actor root had already crossed the
  intended point; it was replaced by retained physical-cleanup and current-B
  join gates.
- The missing-runtime RED initially panicked at spawn; runtime preflight now
  fails before mutation.
- A broad provenance gate found two retired-task spawn sites; they were
  consolidated behind the one private helper before the final full pass.
- `RUSTDOCFLAGS=-D warnings` failed on 15 existing unrelated missing-doc
  warnings. The repository's established plain rustdoc gate passed; unrelated
  documentation was not changed to manufacture a green result.

## Review disposition and next stage

No unresolved P0 or P1 remains in this slice. It is sufficient for the local
post-loss actual-close transaction only. It is not evidence that M2's
one-second interruption test will pass.

Next implement R6b exact-probe silent-failure detection and one
close/probe/deadline winner, then R6d checked `<=500ms` capacity and the
production session-root/nursery lifecycle. Only after those local gates—and
the separate R7-R9 and UDP requirements—pass should macOS TUN, VPS, WAN,
throughput, or M2 acceptance run.

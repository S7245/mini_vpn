# Knife16 R6d checked recovery capacity and production-root local results

Date: 2026-08-25

Status: locally accepted necessary-only R6d result; no production TUN, VPS,
WAN, throughput, M2, or Task-4 acceptance claim

## Outcome

R6d now has one production-shared, crate-private client session root that owns
the real Quinn composition, sole TCP factory, bounded command/flow/effect/timer
storage, monotonic clock, fair scheduler, shutdown drain, and structural join.
It no longer depends on a test manually calling composition methods to keep an
initial real Quinn leg alive, open and drain TCP flows, or obtain the final
ownership witness.

The same stage also closes the checked aggregate recovery-budget contract. A
single `RecoveryBudget` derives both complete hard-failure and reverse-only
paths from the one retained `ReplayHorizon`; every duration addition and every
rate/storage/platform narrowing is checked before actor or root creation.

This is a necessary architecture result, not the end-to-end product result.
`client_tun::run_event_loop` still selects `LegacyOwnedUpstream`. No production
owner daemon, two independent ingresses, R7 reverse-only authenticated switch
policy, R8 fault matrix, R9 real-smoltcp replacement parity, UDP two-leg engine,
WAN capacity, or one-second M2 acceptance is present or tested.

## Capacity contract closed

The hard and reverse paths share one exact retained horizon:

```text
hard_total = normal_ACK_age
           + hard_liveness_after_normal_ACK
           + attach_install
           + replay_to_first_application_ACK

reverse_total = normal_ACK_age
              + reverse_stall_after_normal_ACK
              + authenticated_hint
              + attach_install
              + replay_to_first_application_ACK

hard_total <= retained_horizon
reverse_total <= retained_horizon
```

Exact `500ms` totals pass. Either path at `500ms + 1ns` fails with its named
path and exact values; checked-add overflow fails before construction.
`ReplayCapacitySpec::from_recovery_budget` consumes that exact horizon instead
of reconstructing it, so normal ACK age cannot be double-counted or omitted.

At `100,000,000 bit/s` and `500ms`, the checked directional storage is:

```text
100,000,000 * 0.5 / 8 = 6,250,000 bytes per direction
100,000,000 * 0.005 / 8 = 62,500 bytes per 5ms quantum
6,250,000 / 62,500 = exactly 100 records per direction
```

The root plan checks and retains the command, live-flow, incremental-effect,
replay, message/byte, and timer envelopes. Timer capacity is exactly two fixed
recovery/liveness slots plus one terminal slot per configured tombstone. One
synchronous scheduler turn is capped at `64` category visits; allocator
over-reservation is never used as logical admission authority.

## Production root and flow transaction

`QuinnClientRoot::start_initial` is the sole production-shared constructor for
this slice. It preflights the checked plan, pristine reducer/controller state,
Tokio runtime, real Quinn composition, bounded mailbox/registries, sole TCP
factory, monotonic epoch, progress cursors, and cleanup ownership before Ready.
The factory mint is the final fallible ownership mutation before the prepared
aggregate moves into one root task.

TCP open uses the prepared reducer seam instead of mutating the model and then
trying to compensate:

```text
validate and materialize PreparedLocalOpen
  -> reserve root registry/reply/effect ownership
  -> create the factory pair in the root callback
  -> commit reducer flow/IDs and move exact OPEN
  -> admit OPEN to the current actor FIFO
  -> publish the endpoint reply
```

Retryable actor pressure retains one exact `PendingOpen` and retries fairly.
Permanent post-commit failure remains sticky and owns the aborted endpoint,
driver, frame, and reply until typed cleanup; it cannot be misreported as a
clean open error. If the caller cancels after commit, the root locally abandons
the installed endpoint and drives one ordered `OPEN -> RESET` terminal chain.
The endpoint cannot escape before positive OPEN admission.

The scheduler visits actor inbound/terminal, commands, driver input, effects,
outbound flush, and timers with one category-owned work bound. Actor and TCP
factory progress cursors preserve subscribe/check/park ordering, so an idle
root does not poll or lose a wakeup. Continuous pressure in one category cannot
skip terminal, deadline, or shutdown work.

## Deadline and shutdown semantics

One fixed Tokio monotonic epoch maps production observations into `SimTime`.
Terminal tokens keep one absolute deadline; retry cannot move it forward.
The actor transaction gives transport receive and in-flight publication
precedence over an equal due timer. Semantic terminal expiry consumes only the
session timer and does not poison or cancel the physical actor; liveness-probe
expiry retains its separate fatal behavior.

The publication epoch proves queue/publication ordering, not kernel packet-
arrival time. A record that lacks a same-clock arrival timestamp cannot prove
whether the packet reached the host before a deadline but was serviced later.
That limitation remains explicit and prevents this local timer result from
being promoted into R7, WAN, or M2 deadline acceptance.

Shutdown seals command, factory, driver, sink, and half-close admission before
awaiting cleanup. It adopts already queued/dequeued reset authority without
duplicating it, drains terminal effects and timers, then structurally joins all
current, standby, and retired actors while preserving the first sticky cause.
The direct composition shutdown API remains structural/retryable for frozen
callers; the root-only strict join is the boundary that requires pristine
controller and reducer ownership.

Transport fatal with a live flow now force-drains the exact semantic RESET
tombstone after physical service is unavailable. The fatal cause stays first,
but flow driver, replay, terminal timer, and session tombstone ownership reach
zero. `ClientRootJoined::is_zero` includes reducer/session residual ownership,
not only actor queues and root registries, so a hidden tombstone can no longer
mint a false zero witness.

## Lifecycle cutoff repair from final review

Final review found a real presignal/TOCTOU defect: the lifecycle CAS could move
to `ShuttingDown` before the separate shutdown watch published its wake, while
the root still treated `ShutdownRequested` like Ready or crossed a plain phase
check and serviced one more work item.

The repaired boundary is the lifecycle watch lock itself. Every bounded
synchronous live category holds its read guard for the whole transaction;
shutdown's `send_if_modified` write CAS linearizes either before or after that
turn. Work already dequeued asynchronously uses an input-preserving form. If
shutdown wins, the exact selected command is returned as
`RootNotRunning(ShuttingDown)` without reducer, factory, FIFO, or wire commit.
`ShutdownRequested` skips the live scheduler and enters cleanup without being
misclassified as failure.

Three deterministic real-Quinn tests cover:

- shutdown CAS before the first root poll while inbound is already queued;
- shutdown CAS after Ready, between the fast phase check and inbound turn; and
- shutdown CAS after an OPEN command is selected but before its transaction.

The production-source provenance test also locks the Ready split, both
transaction helpers, the no-`await` synchronous boundary, and exact selected-
command rejection path.

## Review findings repaired

The concentrated implementation and final-review cycle also repaired:

- ACK writer live-message accounting that omitted an in-flight record;
- direct composition shutdown accidentally inheriting root-only pristine
  reducer semantics;
- root actor fatal leaving a semantic terminal tombstone after transport
  service was gone;
- root zero witness omitting reducer/session residual ownership;
- controller FIFO and DELIVERED-waiter state missing from start/join
  quiescence;
- allocator capacity being mistaken for the checked logical effect bound;
- an unbounded caller work budget and mid-batch shutdown delay;
- Ready publication in the lifecycle-CAS/watch-publication gap; and
- the selected-command lifecycle bypass described above.

The first failing lifecycle tracers observed six forbidden scans before Ready
and one extra scan (`6` versus `5`) after Ready. Both become zero-extra-work
tests after the transaction repair. A short Cargo filter that treated the test
filter as a regular expression selected zero tests and was rejected; every
accepted focused and repeat result below names a real nonzero test.

## Accepted local validation

| Gate | Result |
|---|---|
| Focused root storage/clock/lifecycle tests | `17/17` |
| Fixed Quinn actor/composition module | `106/106` |
| Default root library | `1287 passed; 3 ignored` |
| All targets/all features | library `1301 passed; 3 ignored`; main `2/2`; concurrency `10/10; 4 ignored`; provenance `144/144`; protocol `25/25`; session API `1/1` |
| Explicitly ignored concurrency sweeps | `4/4` |
| Real-smoltcp fake adapter | `2/2`; core tracer `100/100` |
| Three lifecycle-cutoff tracers and fatal-live-flow zero-witness tracer | each `100/100` on the final all-feature libtest |
| Development/release all-target/all-feature checks and release build | PASS |
| Strict production-library Clippy | PASS with only the established repository allowances |
| Rustdoc | PASS with established unrelated warnings |
| Isolated vendored Quinn with exact local proto provenance | `47 passed; 3 ignored`; integration `1 ignored`; doctest `1/1` |
| Isolated vendored quinn-proto | `330/330`; doctests `3/3` |
| Independent final correctness/lifecycle/ownership review | unresolved P0/P1/P2 `0/0/0` |
| Rustfmt, `git diff --check`, targeted generated artifacts | PASS |
| Added-line and untracked-file secret-shape scan | PASS |

The final all-feature libtest is
`target/debug/deps/mini_vpn-802b66297ce9f029`, SHA-256:

```text
55820c92d7950e2050415eee9f6eb99a46f81d98a3b28c9975b7d09a3dde832a
```

Vendored Quinn/quinn-proto sources were unchanged after their accepted
isolated gates; the final root-only repair was rebuilt through the repository's
all-target and release gates.

Final implementation and provenance-source SHA-256 values:

```text
ad2786867326063089ad057516c6652e0fd2e4fab329fb21154f0ba04e3b4129  src/owned_upstream/quinn_client_root.rs
a18aa893726cf6f603783dcfa1532837efee72810e0619438777f2b9c4f1b46b  src/owned_upstream/quinn_leg_actor.rs
8670f3e28a64852a7c1f1cf1396b20e56d7dfb3839608a579b89f5e0e774128f  src/owned_upstream/quinn_leg_composition.rs
1cb0c4a703a1f473703c420a65ab98472d797acc037b726cce621bc3ebc6fe58  src/owned_upstream/client_controller.rs
0484d058302f703efbaa35e72dd5a1f7b86b2fc0578dee29410f7aec87b9e69b  src/owned_upstream/leg_io.rs
60fc4a752c3aabdafaa368863eefbcaa933b47673f7c6b3503fa0c0b8dbcd741  src/resumable/capacity.rs
e4a418215fb875a7ad06194c5f960e57710ec84d11c964ff022c8d4fcca6cd8c  tests/knife16_owned_leg_provenance.rs
```

## Scope and next stage

R6d closes the checked recovery-capacity and local production-root lifecycle
prerequisites. It does not complete Task 4 and it does not prove that the M2
one-second uninterrupted interval will pass. The public TUN entry remains on
the legacy upstream, and there is still no real production owner/server or
second ingress.

The next bounded stage is R7: implement reverse-only failure without a local
fault oracle, authenticated switch hints, exact rejection behavior, and the
hard-failure/local-stall/hint trigger race so one switch epoch and at most one
generation commit. Before any R7 deadline or combined-horizon acceptance, the
Quinn actor must capture each inbound record's arrival timestamp in the same
monotonic clock domain at transport-read admission and carry it through queue
and publication; a publication epoch alone is not deadline evidence. R8
fault/fairness, R9 real-smoltcp replacement parity, Task 5 UDP two-leg
delivery, Task 6 owner/Target, production TUN selection, local capacity,
two-ingress WAN qualification, and final M2 remain after it.

Do not run macOS TUN, VPS, WAN, throughput, or M2 yet.

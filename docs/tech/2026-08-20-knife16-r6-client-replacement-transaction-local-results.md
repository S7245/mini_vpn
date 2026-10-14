# Knife16 R6 Client Replacement Transaction — Local Results

Date: 2026-08-20

Status: **LOCAL NECESSARY TRANSACTION ACCEPTED; PRODUCTION REACHABILITY AND
RETIRED-ACTOR JOIN BLOCKED; TASK 4 AND M2 NOT ACCEPTED**

## Scope and verdict

This stage deepens the R6 standby/attach foundation into a production-shared,
crate-private client replacement transaction. It proves locally that an exact
registered replacement or authenticated catch-up can commit one generation,
retire the predecessor once, admit bounded replay through the replacement's
sole FIFO under pressure, preserve inbound ownership behind a barrier, and
recognize recovery only from an exact positive application ACK.

Independent review reports no remaining P0/P1 in the local normal/catch-up
transaction. The result is deliberately necessary-only:

- the real TUN composition still selects `LegacyOwnedUpstream`;
- `ClientSessionController` has no non-test production call site;
- non-test `EncodedLegTransport` has only its fail-closed unsupported default;
- there is no production two-leg actor that owns and awaits the exact leg's
  socket, transport bytes, and structured children; and
- the bounded retired slot therefore cannot yet be cleared safely.

It would be incorrect to describe this as a production switch controller,
complete R6, complete Task 4, a blackout result, or a fix for the M2 one-second
continuity failure.

Superseded-position note (2026-08-24): the fixed physical actor described as
missing below now exists and is locally accepted in
`docs/tech/2026-08-22-knife16-r6-quinn-leg-actor-local-results.md`. The current
blocker is narrower: no production composition root binds that actor's exact
incarnation and joined witness to the controller's exact retired slot. This
historical result still does not authorize TUN, WAN, throughput, or M2.

## Closed local transaction boundaries

### Exact attach and catch-up authority

- Initial attach, registered B attach, status-only C, and catch-up D use
  distinct typed queue admissions. Generic queue push rejects ATTACH,
  ATTACH_ACCEPTED, and generation-status records.
- A registered replacement acceptance retains the exact B request receipt;
  catch-up retains the original B status correlation plus the exact C status
  and D request receipts. Raw attached legs and raw catch-up installers remain
  unavailable in production.
- Accepted-but-uninstalled B/D can consume only an exact terminal or proven
  sole-queue loss into the authenticated status-retry path. Wrong terminal,
  live queue, and pre-delivery cases return all non-cloneable ownership.
- Catch-up remains reducer-legless-only. The controller must first consume the
  exact current terminal and explicitly discard its bounded recovery state;
  it does not synthesize `LegLost` or relax the reducer's Active precondition.

### Precommit capacity and postcommit infallibility

The client recovery bound is derived from configured terminal tombstones `T`,
live flows `F`, client-to-target replay segments `Sc`, and replay bytes `Bc`:

```text
effects <= 1 + T + 3F + Sc
frames  <=     T + 3F + Sc
bytes   <= 38T + 360F + Bc + 37Sc
```

For the focused baseline `F=2, T=8, Bc=128, Sc=16`, this is 31 effects,
30 frames, and 1,744 encoded bytes.

Before reducer commit, the transaction:

- encodes each recovery frame once into an owned `Bytes` backing;
- validates exact count, sum, largest frame, and offset/range arithmetic;
- reserves the bounded effect/range containers;
- reserves queue slots/bytes and an ordinal span;
- reserves lifetime LegIo message/byte capacity; and
- holds the exact endpoint terminal transition across final open recheck,
  reducer commit, predecessor retirement, and recovery publication.

After commit, the controller only moves prebuilt ownership. Pressure retry
uses the same `Bytes` backing and ordered-delivery state; it does not encode,
copy the backing, allocate a new recovery container, or rerun the reducer.

### Admission, ordered delivery, and application recovery

The implementation keeps three facts separate:

1. `AdmissionCompleted`: all recovery Transmit records entered the exact B/D
   FIFO. This releases the inbound admission barrier.
2. `ClientRecoveryTail::actual_delivery_complete`: every exact ordered record
   received its transport delivery completion. Submission alone is
   insufficient, including when submit capacity has already reached zero.
3. `ReplacementRecovered`: the reducer accepted a positive application ACK
   whose session, B/D generation, flow, direction, and half-open offset delta
   intersect one of the exact precommit client-local replay ranges.

Equal/duplicate ACK, later positive ACK after the one-shot fact, stale A,
wrong generation, idle replacement, and a new flow ACK arriving before the old
flow are inert. The opaque driver input can be minted only by the real bounded
driver dequeue, so a sibling-created peer-frame command cannot bypass this
observation path.

### Pressure, inbound, and terminal ownership

- With `max_frames=1`, a held first record plus pressure on the second rearms
  when exact ordered delivery clears even if the physical flush returns no new
  submission. Replay remains FIFO and the reducer generation commits once.
- While Recovering, inbound B/D delivery is returned byte-for-byte before
  route/lane checks, decode, or any LegIo counter mutation. The same value is
  consumed exactly once after admission completes.
- Exact current terminal binding is two-phase for all current shapes:
  Recovering reservation, Active with an undelivered recovery tail, and
  Active with no recovery tail. Wrong terminal restores the complete current
  state. Exact terminal moves current B/D into a distinct terminating state
  while retaining predecessor A in the sole retired slot.
- Only explicit same-controller discard releases the exact recovery
  reservation/tail, drops the current queue, and applies one same-supervisor
  `LegLost`, which must produce exactly one `ResumeGraceStarted`. Queue or
  endpoint loss alone cannot mint this authority.
- Late retired-A DATA, ACK, CLOSE, and RESET are classified and discarded
  without creating reducer events or changing the active replacement.

## Source closure

- Controller install, initial bootstrap, and observed-apply gates have private
  fields and private mint sites.
- The production initial bootstrap is one by-value entry; there is no
  activation-to-controller raw window.
- Registered normal and catch-up accepted wrappers have no raw `into_parts`
  escape. Standby-private, non-cloneable gates are consumed at each response,
  derivation, assembly, and install boundary.
- Standalone provenance shims are private uninhabited enums in the integration
  test crate. They preserve type-check coverage without constructing
  production authority.
- The final source scan includes the new untracked controller file explicitly;
  no PEM/private key, access/API token, password, dependency, or URL change is
  present. Debug output continues to redact capability and credential
  material.

## Local validation

Accepted final-tree gates:

- `cargo test --all-targets`: library `1096/1096`, `3` ignored; main `2/2`;
  typed provenance `125/125`; protocol `25/25`; public session API `1/1`;
- concurrency harness with `--features harness`: `10/10`, `4` ignored, plus
  the four ignored sweeps explicitly `4/4`;
- controller `10/10`, standby `20/20`, LegIo `28/28`, supervisor `46/46`,
  TCP driver `42/42`, and the focused normal/catch-up/terminal/ACK tracers;
- real-smoltcp retained Task-3 parity floor with `--features harness`: `2/2`,
  then the same frozen binary `100/100` consecutive runs;
- `cargo check --all-targets --all-features` and the release equivalent;
- release build;
- strict production-library Clippy with the four documented legacy
  allowances;
- rustdoc PASS with 12 existing unrelated warnings;
- vendored Quinn from an isolated exact source graph resolving the absolute
  local `quinn-proto 0.11.16`: `40/40`, `3` ignored, doctest `1/1`;
- vendored quinn-proto: `330/330`, docs `3/3`;
- `cargo fmt --all -- --check`, `git diff --check`, source provenance, and
  changed-secret-shape scan.

Rejected evidence, recorded in `.learnings/ERRORS.md`, includes an unpatched
vendored Quinn invocation that selected registry proto `0.11.17`, a
real-smoltcp filter that omitted the harness feature and ran zero tests, and a
source-string marker that lagged an equivalent Clippy tuple-constructor
rewrite. Generated vendored lock/target artifacts were removed after the
accepted gates.

## Production blockers

### 1. No production composition root

The real client TUN path still constructs `LegacyOwnedUpstream`. The new
controller and working encoded transport are exercised only by deterministic
local harnesses. A release build proves dormant code compiles, not that real
traffic reaches it.

### 2. No exact transport actor join authority

`LegTransportReporter` proves an exact monotonic terminal fact, not that an
actor has stopped. The tree has no production owner binding a separate actor
incarnation to the exact authenticated leg, cooperative cancel, a real
`JoinHandle`, the socket/held bytes, and all structured children.

The retired slot must remain occupied until a future fixed actor loop has
actually awaited root and bounded children and can mint an exact joined fact.
Terminal, cancel request, channel close, `is_finished`, timeout, abort, drop,
weak-reference disappearance, or deterministic wire cleanup are not that
fact. Panic/JoinError must fail closed and retain the slot.

### 3. Production-enable hardening

Before a real composition root is enabled, `LegIo::for_authenticated_transport`
must be narrowed behind the concrete actor-owned factory. Dormant sibling
surfaces for raw EstablishedLeg binding, raw queue enqueue, memory ingress,
and supervisor-only owner attach should also gain compiler-enforced private
capabilities rather than relying only on module convention/source counts.

## M2 decision and next step

This result cannot answer the one-second M2 continuity acceptance with PASS.
It closes necessary local ordering, ownership, pressure, catch-up, and ACK
boundaries, but the path is not production-reachable and has not crossed a real
socket actor, TUN, two-ingress WAN, or the M2 workload.

The next legitimate vertical is a fixed non-test two-leg transport actor and
composition root, not another controller-only token:

1. bind an independent actor incarnation to the exact authenticated leg;
2. make the fixed actor own the encoded transport, socket, held bytes, and all
   bounded children;
3. retain cooperative cancel and the real join handle outside the policy
   controller;
4. mint exact joined authority only after successful await of all ownership;
5. consume that fact once to clear the retired slot without `LegLost` for the
   already-retired predecessor; and
6. then run a deterministic real-loopback actual-close/blackout tracer before
   any macOS TUN, VPS, WAN, throughput, or M2 acceptance.

Until that vertical passes, Task 4 and R6 production closure remain blocked.
Even after it passes, all R7a–d reverse-only no-oracle, authenticated-hint,
rejection, and trigger-race gates plus R8/R9 remain separate and open. WAN,
throughput, M2, and M3 have not run or been solved by this local result.

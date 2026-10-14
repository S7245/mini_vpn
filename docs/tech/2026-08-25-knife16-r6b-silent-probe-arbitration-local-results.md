# Knife16 R6b Silent-Probe Arbitration Local Results

Date: 2026-08-25

Status: **LOCALLY ACCEPTED AS A NECESSARY-ONLY R6b SLICE; TASK 4, WAN, AND M2 ARE NOT ACCEPTED**

Architecture contract:
`docs/tech/2026-08-25-knife16-r6b-silent-probe-arbitration-architecture-spec.md`.

## Outcome

R6b's local silent-failure transaction is closed. The controller now owns one
exact current-A probe and timer token, while the composition owns registered B
before registration authority or its first registration byte can escape.
Probe time begins only after the real ordered Quinn writer positively accepts
the complete record. Only an authenticated ACK matching the exact physical
seal, session, generation, epoch nonce, sequence, and switch epoch can consume
that probe.

Both current-A and registered-B ACK entry points enforce their own receive-time
deadline. An ACK at the exact deadline is accepted; an ACK at deadline plus
one nanosecond is rejected even if timer driving is delayed. A late current-A
ACK restores the same armed ownership, so the still-due opaque token can commit
the loss rather than disappearing behind scheduler order.

Probe expiry and spontaneous transport failure share the actor's existing
first-failure transaction. If expiry wins, the actor publishes the semantic
`ProbeDeadlineExpired` terminal after fixed actor message/byte/DELIVERED
ownership reaches zero and before the physical connection/Endpoint/socket
close tail finishes. The existing controller path then emits exactly one
`LegLost`/`ResumeGraceStarted`; no timer or test blackout oracle constructs a
session event or terminal directly.

The accepted real-Quinn tracer proves the complete local chain:

1. B is authenticated, registered, probed, and made fresh under composition
   ownership before A is declared lost.
2. A's exact probe is physically delivered, a late ACK is rejected, and the
   same opaque deadline token wins the actor first-failure race.
3. A becomes the exact terminal-bound retired source while its physical close
   tail remains blocked. B installation does not wait for that tail.
4. The signed ATTACH request and authenticated `ATTACH_ACCEPTED` retain one
   pointer-identical causal delivery receipt. A wrong response cannot mint it;
   the later local DELIVERED publication is idempotent.
5. Nine replay bytes travel only through installed B, reach the existing
   Target, and the owner reducer's first positive application ACK advances
   `ReplacementRecovered` from `0` to `9` and releases only that replay range.
6. Only after the application ACK may exact zero-ownership joined A clear.
   Current B, Target state, and unrelated ownership remain live and unchanged.

This is a non-empty end-to-end recovery proof. Registration, probe ACK, Quinn
DELIVERED, installation, and a zero-replay branch are not treated as recovery
success.

## Ownership and shutdown closure

- Registration Pressure and Invariant failures retain the exact actor and
  pending registration state. Failure-only boxing satisfies strict Clippy
  without adding a success-path allocation or changing rollback identity.
- Every async join path preflights the Tokio runtime before phase mutation,
  cancellation, or ownership extraction. Cancelling the outer waiter loses
  only observation; the composition retains the structured task.
- Shutdown independently attempts standby, current, and retired cleanup. A
  permanent standby `TaskLost` remains the first sticky error but cannot
  prevent current or retired cancellation and structural join.
- Install disposition is exhaustive: retry bounded pressure, fail the
  session, clean a terminal B, or stop on an invariant. Deterministic recovery
  or integrity failure cannot be relabeled as pressure.
- Successful Joined and an observed structural root error take precedence
  over the cleanup-only Closing marker. The first semantic or structural error
  remains stable across repeated join attempts.

## Capacity and reachability classification

R6b adds fixed-capacity probe/ACK/timer state and at most one structured B join
task. It adds no payload queue, payload copy, pacing rule, D16 credit, TUN
writer, or throughput mechanism. The existing 100-Mbit/s, 500-ms per-direction
payload baseline therefore remains unchanged at 6,250,000 bytes before frame
geometry.

This repair is necessary for a bounded silent switch but is not sufficient for
the next throughput or interruption gate. The production TUN entry still
selects `LegacyOwnedUpstream`; no production session root schedules probes or
owns the peer responder. Static/local evidence therefore cannot establish a
one-second M2 result.

## Deterministic TDD and review repairs

The first GREEN was deliberately not accepted unchanged. TDD and concentrated
review found and repaired these false proofs or material defects:

- The initial tracer waited on the response-side delivery writer instead of
  the request-side local receipt. The corrected gate withholds the actual
  ATTACH request DELIVERED publication.
- A zero-replay tracer could clear retired A without proving Target continuity.
  It was replaced by the nine-byte replay plus Target-backed application-ACK
  transaction above.
- Registered-B ACK validation originally trusted timer-drive order. Review
  added receive-time validation and exact deadline/deadline-plus-one tests.
- Current-A ACK validation had the same independent defect. A RED showed a
  deadline-plus-one ACK could cancel the token; the controller API now checks
  receive time locally, rejects it, and retains the due token.
- Broad Quinn tests exposed two error-precedence bugs: clean Joined could be
  hidden by Closing, and an observed root join failure could be obscured by
  cleanup state. Preflight observation and explicit precedence repaired both.
- Full typed-provenance initially failed because whole-file source counts had
  become stale. The repair scopes assertions to the named production seams;
  it does not lower non-Clone, constructor, or single-owner invariants.
- Strict Clippy exposed three large failure variants. Only cold error ownership
  is boxed; retry moves the original value back out and the successful path
  remains allocation-free.
- The first isolated vendored build exhausted a 49-GiB accumulated Cargo target
  and failed with `No space left on device`. Only reproducible Cargo build
  output was cleaned. The accepted run used an isolated target with
  incremental compilation disabled and passed every vendored gate.
- An initial secret-scan wrapper had shell quoting and no-match/pipefail bugs.
  The accepted gate scans added lines and untracked files without printing
  matches; repository-tracked development certificate fixtures and unchanged
  historical environment examples are outside this stage diff.

No TUN, VPS, WAN, throughput, M2, M3, user traffic, or external resource was
used by any RED, repair, or gate.

## Accepted local validation

| Gate | Result |
|---|---|
| Focused Quinn actor/composition | `71/71` |
| Focused registered-standby state | `27/27` |
| Focused client controller | `18/18` |
| Focused `LegIo` | `28/28` |
| All targets/all features | library `1196 passed; 3 ignored`; main `2/2`; concurrency `10/10; 4 ignored`; provenance `130/130`; protocol `25/25`; session API `1/1` |
| Explicitly ignored concurrency sweeps | `4/4` |
| Real-smoltcp fake adapter | `2/2`; core tracer `100/100` |
| Seven critical R6b lifecycle tracers | each `100/100` on the final frozen libtest |
| Strict production-library Clippy | PASS with only the four established repository allowances |
| Release all-target/all-feature check and release build | PASS |
| Rustdoc | PASS with `15` established unrelated warnings |
| Isolated vendored Quinn | `47 passed; 3 ignored`; integration `1 ignored`; doctest `1/1` |
| Isolated vendored quinn-proto | `330/330`; doctests `3/3` |
| Rustfmt, `git diff --check`, generated artifacts | PASS |
| Focused changed/untracked/add-line secret shapes | PASS |

The final all-feature libtest is
`target/debug/deps/mini_vpn-802b66297ce9f029`, SHA-256:

```text
1529c66b41f51ea616cea4e0f2353bab6f23a440b3c6f650ecafcf6b6296ad57
```

The seven `100/100` sets on that exact binary are the complete silent-A to
fresh-B nine-byte recovery transaction, late registered-B ACK rejection,
no-runtime shutdown preflight, permanent standby TaskLost cleanup isolation,
registration Pressure ownership, registration Invariant ownership, and the
registered-B exact-deadline/deadline-plus-one boundary.

Accepted source SHA-256 values are:

```text
4b9cec6cf9da01a08f46775826fc443a31868ad9026f090f389498ae8bc5aedd  src/owned_upstream/client_controller.rs
bb61bf9250737af4bde2b15fce3db8296ec420798da0882024d7d1655b98fe68  src/owned_upstream/standby.rs
274d274728edb9e1fa41b041b577b0e58b14dab1d16e1e943ea96287f29e93ef  src/owned_upstream/quinn_leg_actor.rs
3bfdf9232c30f1ed3fa8bdddc0db1bb219c7be0f740f805b431c7212684bfebb  src/owned_upstream/quinn_leg_composition.rs
f34fdaf8825748829664f32fcf35003022fd27d9a086d9f79c1574c3e26209b7  tests/knife16_owned_leg_provenance.rs
```

Independent final-tree review reports unresolved P0/P1/P2 **0/0/0**.

## Remaining boundary and exact next stage

R6b is locally accepted, but Task 4, production reachability, WAN, throughput,
and M2 remain open. The exact next bounded stage is **R6c late-record closure**:
prove that late or cross-incarnation DATA, application ACK, CLOSE, RESET,
ATTACH acceptance, generation status, probe ACK, and local DELIVERED records
cannot resurrect A, refresh B, forge recovery, release replay, or disturb the
active Target.

After R6c, R6d must close the checked end-to-end `<=500ms` component sum,
aggregate recovery capacity, production probe responder, and structured
session-root/nursery lifecycle. R7-R9 reverse-only/no-oracle switching,
enhanced owner/Target, UDP two-leg behavior, real TUN integration, WAN, and M2
still follow. Do not run macOS TUN, VPS, WAN, throughput, or M2 from this
result.

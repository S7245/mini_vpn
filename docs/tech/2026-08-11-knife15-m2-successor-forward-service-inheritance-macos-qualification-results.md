# Knife15 M2 Successor Forward-Service Inheritance macOS Qualification Results

Date: 2026-08-11

Status: **PAIRED QUALIFICATION TRAFFIC AND SAFETY EVIDENCE PASS; RECORDED FAILURE IS A RUNNER FALSE NEGATIVE; FORMAL M2 IS REOPENED; M3 REMAINS BLOCKED**

Exact Mac source: `25ea39cccca664c25624c1e69dcfbb78d973aed8`

Reviewed runner repair: `3a8a796`

Mac artifact:
`/tmp/mini_vpn_knife15_macos_20260811_054300.tar.gz`
(SHA-256 `23a49673538b2a8be6ddb516a77123fdfdf8da2e7afc137f9c2df2873214fe3c`).

Paired Exit artifact:
`/tmp/mini_vpn_knife15_exit_target_observer_20260811_054205.tar.gz`
(SHA-256 `6f745e9f9c37e540b78e4e74d874dcb94d0720af94a74462ddb139d5602b0c2b`).

Local architecture result:
`docs/tech/2026-08-11-knife15-m2-successor-forward-service-inheritance-local-results.md`.

## Result

The exact-source Mac passed baseline `11.858/55.917 Mbit/s`, the bounded
direct discriminator at `5.928 Mbit/s` with zero sender and receiver
intervals, start, smoke, every preflight, two exact qualification cycles,
all eight phases, two DNS checks, two real-client checks, and cleanup. The
six TCP results had one sender-zero interval and zero Target receiver-zero
intervals. Maximum sender/receiver byte gap was `5,636,096B`; maximum UDP
loss was `1.830049%`.

The immutable artifact records `m2-qualification.status=failed` and verdict
`MISMATCH`. That status is retained as provenance, but it is not a data-plane
failure. Immediately after the final real-client probe, the newest Endpoint
statistics line still held a transient reservation at
`available/live/outstanding=60,031/1,409/0B`. The runner sampled once and
called terminal safety immediately. The next ordinary statistics line about
14 seconds later was `61,414/0/0B`; every later line remained zero-owned, and
cleanup passed.

Replaying the exact artifact prefix through terminal safety returns dirty.
Adding only the naturally subsequent bounded-drain records returns PASS.
Restoring the schedule's pre-check `PASS_NON_ACCEPTANCE` status in a separate
analysis copy also makes the exact result SLO and every terminal-safety
predicate pass. The archive itself was not modified.

## Paired Exit Classification

The Exit observer captured `3,270,562` packets with zero kernel drops. The
cycle-2 short-forward connection corresponding to the one Mac sender-zero
interval supplied `2,817,041B` to Target over `10.182s`. Its maximum positive
supply gap was only `263.147ms`; every full Target receiver interval remained
nonzero. This proves the sender-zero sample was absorbed by already-owned
pipeline data and did not become a Target continuity failure.

Three `remote_write_failed` relay closes were timed-transfer close tails.
Every canonical close had D16 queued/leased/reserved `0/0/0B`, all phases had
already completed, terminal ownership replay passed, and no
`stalled_write_timeout`, `idle_timeout`, TUN/interface error, Endpoint
would-block, conservation violation, route/DNS failure, or cleanup mismatch
occurred.

No connection path reset, generation replacement, or nonzero inherited
forward-service floor occurred in this qualification. Therefore the run proves
the reviewed production tree is regression-clean under two paired mixed
cycles; it does not by itself claim runtime reachability of the new inheritance
branch. Formal M2 remains the deciding gate for that branch.

## Runner Repair

`3a8a796` replaces the immediate qualification terminal-safety assertion with
a bounded wait using the existing `duration + 30` controlled-drain limit
(`50s` under the frozen profile). A persistent safety failure still returns
failure at the deadline. After a clean drain, the runner rechecks the live
process, watchdog, routes, DNS, full-tunnel ownership, recent network control,
and sample-count evidence so the wait cannot conceal a runtime failure.

The focused runner RED used the observed `60,031/1,409/0B` line followed by a
clean `61,440/0/0B` line. It failed because no wait seam existed. The GREEN
passes after the clean update, and a second fixture proves a permanently dirty
line fails at the bounded deadline. The exact artifact replay independently
passes after its real drain record.

## Gates And Review

```text
Mac archive SHA-256:                  PASS
Exit archive SHA-256:                 PASS
Exit internal SHA256SUMS:             PASS
baseline/direct/smoke/preflights:      PASS
two cycles / eight phases:             PASS
Target receiver-zero intervals:       0
Endpoint conservation/terminal drain: PASS
network/interface/route/DNS/cleanup:   PASS
runner shell syntax:                   PASS
runner full self-test:                 PASS
artifact terminal-safety replay:       PASS
diff / changed-content secret scan:    PASS
code review:                           PASS; no unresolved P0/P1
formal M2 acceptance:                  NOT_RUN
```

No Rust data-plane code, D16, MTU, pool, QUIC window, chunk, Cubic, GSO,
Endpoint pacing, self-wake, workload, or SLI changed in this repair.

## Next Gate

Do not repeat or tune qualification. Pull and rebuild the reviewed descendant
of `3a8a796`. When the HK Mac and `.33` Exit/Target services can remain
uninterrupted for about 25 hours, take exactly one fresh:

```text
m2-ipv6-check -> baseline -> direct-discriminator -> start -> smoke ->
m2 -> status -> stop
```

Preserve `status/snapshot/stop` evidence after any failure. A Target
receiver-zero after a logged nonzero inherited floor rejects the inheritance
mechanism; do not tune or repeat unchanged. M3 remains blocked until formal
M2 and cleanup pass.

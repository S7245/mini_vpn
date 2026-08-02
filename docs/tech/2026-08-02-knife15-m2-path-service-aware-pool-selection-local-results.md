# Knife15 M2 Path-Service-Aware TCP Pool Selection Local Results

Date: 2026-08-02

Status: **LOCAL COMPLETE — real M2 acceptance pending; M3 blocked**

## 1. Bundle Result

The recovered immutable HK artifact is:

```text
d4fc3bc6d47467e51864ff62fff82ba7a926ff322aa65e302decd9c771f1b31e
/tmp/mini_vpn_knife15_macos_20260801_053127.tar.gz
start-owned source: 6f1df4a
```

The archive hash, safe paths, source provenance, and secret scan passed. The
`cebf30d` cleanup repair also passed its real discriminator: with the owned
utun already absent and all affected routes exactly restored to the recorded
physical interface/gateway, it released only the stale low/high/fake markers,
performed no foreign route deletion, restored DNS, stopped the process, and
finalized cleanup evidence.

M2 itself is not accepted. Baseline/direct, start/smoke, IPv6 and controlled
full-tunnel activation, public Exit, fake-IP HTTPS, cycle 1, and cycle 2's
first three phases passed. Cycle 2 `short-forward-1` then had a first complete
`1.001225s` Target receiver interval of `0B` and failed closed.

## 2. Selected Failure Class

The failure was not a final partial interval. Exact-window Exit and gateway
probes stayed lossless, routes and `en0` stayed stable, TUN/D16/Endpoint
ownership stayed conservative, and the selected QUIC connection continued
authenticated RX/ACK progress.

The decisive open sequence was:

```text
control -> conn1 at active_before=60
data    -> conn0 at equal active_before=62 by stable index

conn0 path: cwnd=10,124B, RTT=163ms
conn1 path: cwnd=23,842B, RTT=163ms
```

The failed conn0 D16 writer waited `3,355,211us`, versus `761,192us` for the
comparable passing first-cycle short-forward on conn1. This selects a narrow
pool-placement defect: lease counts correctly describe ownership, but equal
nonzero counts do not distinguish the current QUIC send service available to
a new flow.

Rejected branches were observer-tail waiver, operator error, Exit outage,
physical route failure, TUN/Endpoint/D16 loss, dead QUIC connection, pool-size
change, rebind tuning, pacing/MTU/window/chunk tuning, and workload/SLO tuning.

## 3. Implemented Policy

`TcpPoolLeaseSelector` now uses this exact ordering:

1. minimum active/opening lease count;
2. if that minimum is zero, stable lowest index;
3. for an equal nonzero minimum, greater known `cwnd / RTT` path service;
4. known before unknown; exact-equal or all-unknown keeps stable index;
5. existing preparation CAS and active-count CAS remain authoritative.

Quinn-specific stats stay in the outer adapter. The selector receives a plain
`TcpPoolPathService` value and compares ratios by exact `u128` cross
multiplication, introducing no threshold, smoothing constant, or weight.
Locked, closed, zero-cwnd, zero-RTT, and missing observations are unknown.
Sampling never awaits a connection lock and is repeated after a preparation
wait wakes, so a waiter does not reuse the pre-wait observation.

Diagnostics now publish:

```text
policy=least_active_then_path_service
path_cwnd=<bytes|unknown>
path_rtt_us=<microseconds|unknown>
path_service_tiebreak=<true|false>
```

No payload-hot-path logging or periodic sampler was added.

## 4. TDD And Review

The focused equal-busy-load test was RED against the old selector because no
path-service input or result fields existed. The minimum GREEN implementation
then selected conn1 for the exact observed `10,124/163ms` versus
`23,842/163ms` discriminator.

Focused coverage locks down:

- exact ratio comparison and zero/overflow-safe conversion;
- idle `conn0 -> conn1` determinism despite unequal stale samples;
- least-load precedence over path service;
- known-versus-unknown and exact-equal stable fallback;
- simultaneous reservation, preparation exclusion, cancellation/drop,
  saturation, and wake-up;
- fresh sampling after a preparation waiter wakes;
- additive diagnostic compatibility.

The stage code review found and repaired one pre-acceptance concurrency issue:
the first implementation reused a pre-wait vector after every slot was busy
preparing. Sampling now occurs inside every selector retry. Final review found
no unresolved P0/P1.

## 5. Capacity And Regression Gates

The frozen Endpoint architecture still provides approximately
`239.167 Mbit/s` application capacity. The 32 MiB real Quinn/TUN/D16 capacity
test, whose code hard-fails at `<=170 Mbit/s`, passed with exact clean EOF and
ownership assertions. The pool decision remains a TCP-open slow-path action:
it allocates at most the frozen pool-size sample vector and performs only
nonblocking stats reads, not per-payload work.

Final local gates:

- focused TCP-pool tests: `22/22` PASS;
- root library: `673 passed / 3 ignored` PASS;
- root binary: `2/2` PASS;
- integration: `10 passed / 4 ignored` PASS;
- release build and all-target Clippy: PASS; existing unrelated warnings only;
- Knife15 internal/external runner self-tests and Bash syntax: PASS;
- Knife14 low-RTT, US-client, and sing-box-control self-tests: PASS;
- vendored Quinn with exact local quinn-proto patch: `37 passed / 3 ignored`,
  integration `1 ignored`, doc `1/1` PASS;
- vendored quinn-proto: `309/309`, doc `3/3` PASS;
- formatting, diff, tracked/untracked, and changed-content secret scans: PASS.

One initial standalone Quinn command omitted the required local
`quinn-proto` patch and therefore compiled the registry crate, which correctly
failed on fork-only APIs. The corrected dependency-provenance command proved
the exact vendored path and passed; this was a gate-command error, not a
product regression.

## 6. Acceptance And Stop Rule

This local stage is intended to be sufficient only for the observed
equal-nonzero-load/lower-service placement class. It is not proof against all
WAN loss and it does not accept M2.

The next action is exactly one fresh HK formal M2 from this reviewed source or
a descendant, with a rebuilt release binary and the unchanged runbook:

```text
IPv6-off/read-only check -> baseline -> direct-discriminator
-> start -> smoke -> m2 -> status -> stop
```

If it fails after selecting the greater-service equal-load slot, reject this
hypothesis and inspect the exact stream/path controls. Do not tune constants,
waive the receiver SLI, increase the pool, or repeat unchanged. M3 remains
blocked until the complete formal M2 plus stop cleanup is accepted.

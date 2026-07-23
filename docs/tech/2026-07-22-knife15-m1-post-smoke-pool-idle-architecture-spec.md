# Knife15 M1 post-smoke TCP-pool idle barrier architecture spec

Date: 2026-07-22

## Stage goal

Make the formal M0/M1 workload start from a proven idle TUIC TCP pool after
the mandatory tunnel smoke, without changing the data plane, workload, or
acceptance SLO.

The exact-source `f60926e` HK bundle
`/tmp/mini_vpn_knife15_macos_20260722_110059.tar.gz` (SHA-256
`9071ec0fb8bf832714000bddf14a843714536aeaa422217c47b89bab2532ceca`)
failed the first M1 forward phase because its first complete Target receiver
interval was zero. The local sender delivered `457,048,064B` in `300s`; the
Target received `456,523,776B`, an exact `524,288B` gap. This was not a short
command tail.

The user sequence, source, baseline, direct discriminator, routes, gateway,
physical interface, TUN ownership, Endpoint conservation, resources, and
cleanup were correct. The delayed `stop` happened about fourteen hours after
the already-recorded phase failure and did not cause it.

## Selected boundary

Smoke completed at `11:01:45Z`, M1 was prepared in the same second, and its
first phase started at `11:01:46Z`. The smoke reverse data relay on conn1 still
owned both native relay-half leases when M1 opened:

1. M1 control reserved conn0 from `active_before=0`.
2. conn0 and conn1 then both exposed two live half leases.
3. the stable least-active tie selected conn0 again for M1 data with
   `active_before=2`.
4. the old conn1 smoke relay was reaped only after the M1 data open.

Three comparable earlier HK M1 starts placed the new control/data pair on
conn0/conn1 because the smoke close tail had already drained. Their first
Target receiver interval was positive. This run is therefore a test-stage
lifecycle race, not evidence that the frozen pool size or selector policy
should be tuned.

The first failing phase also shows why the stream-ACK-qualified rebind repair
must remain unchanged. Same-stream ACK progress continued and correctly
suppressed a rebind. QUIC ACKs prove ingress to the Exit, not that the Exit has
already written bytes to the Target TCP socket.

## Capacity and reachability gate

The M1 offer was `12,185,593 bit/s`, or `1,523,199.125 application B/s`.
Endpoint pacing owns `30,720,000 wire B/s`, over twenty times this application
rate, and the sender sustained the offer. Conservation never exceeded
`61,440B`. Capacity, pacing, D16 admission, and TUN service are not the failed
boundary.

The relevant path is:

```text
iperf client -> macOS TUN -> smoltcp -> D16 relay writer
-> TUIC send stream -> Quinn Endpoint -> sing-box Exit -> Target TCP receiver
```

The repair is a pre-workload lifecycle gate around this path, not a throughput
mechanism. It is intended to be sufficient only for the observed smoke-to-M1
race. A complete user-operated M1 remains necessary for release acceptance.

## Architecture

The existing 250ms Endpoint monitor already obtains the exact total from
`TcpPoolLeaseSelector::active_total()`. Publish a log record only when that
total changes:

```text
tuic-tcp-pool-activity active_leases=N
```

The count is deliberately named `active_leases`: one native relay owns a lease
in each half so the count remains nonzero until both halves have dropped. Zero
is the required quiescence invariant.

After forward smoke, reverse smoke, and fake-IP DNS complete, the macOS runner
waits until the latest activity record is zero. The wait reuses the existing
smoke hard timeout (`DURATION + 30s`), records the successful barrier, and
fails closed while leaving the TUN up if ownership does not drain. Formal M0
and M1 independently require the latest record to be zero before creating
their evidence directories.

## Invariants

- A formal soak phase cannot start while any smoke TCP relay half owns a pool
  lease.
- Missing, malformed, or nonzero activity evidence is not idle evidence.
- The monitor logs only transitions, not every 250ms sample.
- No fixed sleep is used as a correctness assumption.
- The strict receiver-zero SLI and partial-tail exception are unchanged.
- D16, MTU1200, UDP1160, pool2, QUIC windows, chunks, Cubic, GSO, self-wake,
  Endpoint pacing/recovery values, M1 schedule, and rates remain frozen.

## Failure discriminators

- Barrier timeout plus nonzero latest leases: relay/close lifecycle failure;
  preserve status/snapshot/stop evidence.
- Barrier passes but the next pair co-locates because of non-test traffic:
  runner/environment isolation failure.
- Barrier passes, pair is separated, and a Target interval is zero with an
  ACK-stalled writer: path recovery branch.
- Barrier passes, same-stream ACK advances, and Target still has a complete
  zero interval: Exit-to-Target delivery branch, not Endpoint rebind.

## Non-goals and stop rule

Do not change selector tie-breaking, release a live half lease early, add a
fixed grace interval to iperf evidence, relax the zero-interval SLI, or tune any
frozen data-plane/workload value. If the local activity contract or runner
barrier cannot be made deterministic and fail closed, stop before requesting
another HK M1.

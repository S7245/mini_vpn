# Knife15 M2 24-Hour Real-Client Soak Local Results

Date: 2026-07-30

Status: **LOCAL PASS — user-run HK macOS M2 pending**

## Outcome

The Knife15 macOS runner now has one formal `m2` action for a controlled
24-hour IPv4 full-tunnel soak. It preserves the accepted Knife14 H10d16 data
plane and M1 SLOs while adding runner-owned split-default routes, system DNS,
real HTTPS/fake-IP/public-egress evidence, six lifecycle checkpoints, bounded
storage/resource gates, and two-phase cleanup acceptance.
The implementation commit is `4dac87c`.

No Rust data-plane code, Endpoint pacing value, MTU, pool, QUIC window, chunk,
Cubic, GSO, self-wake, M1 SLI, or workload-rate cap changed.

## Implemented Contract

The formal traffic/drain budget is exactly `86,400s`:

```text
steady-a 14400 -> idle-1 600
quiet-a 10800  -> idle-2 600
steady-b 14400 -> idle-3 600
churn 10800    -> idle-4 600
quiet-b 10800  -> idle-5 600
steady-c 21600 -> final 600
```

The immutable count model requires:

```text
6 active windows
5 idle/resume pairs
1 final drain
93 complete cycles/DNS/real-client probes
934 TCP results
95 UDP results
1029 total phase results
6 checkpoints
```

Partial cycles at active-window boundaries retain their TCP/UDP evidence but
do not gain DNS/real-client completion. A separate complete-cycle identity
keeps the 93 real-client files contiguous even though phase-attempt numbers
correctly contain those partial-cycle gaps.

## Full-Tunnel And Real-Client Boundary

`start` remains target-only. `m2` alone:

1. records the physical Exit interface/gateway and active network-service DNS;
2. blocks any routable physical IPv6 path;
3. pins the TUIC Exit to the physical gateway;
4. adds `0.0.0.0/1`, `128.0.0.0/1`, and `198.18.0.0/15` to the owned utun;
5. points the physical service DNS at `DNS_TARGET` and flushes the resolver;
6. proves public egress, fake-IP resolution, HTTPS, routes, DNS, and controls
   before registering the 24-hour workload.

Every probe removes proxy environment variables and uses the system resolver;
it never uses `--resolve`. Public egress must equal the TUIC Exit address, and
both HTTPS remote addresses must be in `198.18.0.0/15`.

Cleanup is idempotent and ownership-aware. It removes only matching M2 routes,
restores the exact protected DNS snapshot, and refuses to overwrite an
unexpected external route/DNS owner. `m2_slo_evidence=PASS` is pre-stop only;
`formal_m2_acceptance=PASS` is impossible until `stop` proves route, DNS, TUN,
process, watchdog, and bundle cleanup.

## Lifecycle And Capacity

M2 requires at least `4GiB` free before activation and retains the existing
`1GiB` watchdog floor, `256MiB` log hard limit, and no-compaction acceptance
rule. It requires at least 2,700 process, interface, network, and Endpoint
samples.

Six drain checkpoints require Endpoint live/outstanding bytes, active relays,
and active fake-IP ownership to be zero. The registered fake-IP cache must stay
stable at one or two entries. Requiring zero registrations would be impossible
under the frozen `1,800s` fake-IP TTL and a `600s` drain, so the accepted
discriminator is zero active ownership plus a non-growing two-host cache.

## TDD And Review Repairs

Focused fixtures cover route/DNS parsing, mutation order, partial rollback,
idempotent cleanup, IPv6 blocking, immutable profile/count math, child
timeouts, real-client metadata and egress, compressed scheduling, checkpoints,
summary acceptance, prior-stage behavior, and secret/one-shot cleanup.

Review found and repaired these release-blocking edges:

- sender-only zero intervals are retained as review evidence instead of
  falsely failing the receiver-only M2 SLO;
- real-client labels must match their filenames;
- complete-cycle labels use their own contiguous identity rather than the
  phase-attempt identity that includes partial window tails;
- an unknown IPv6 route-command failure fails closed instead of being treated
  as “no IPv6 route”;
- `direct-discriminator` binds `M2_BASELINE_DIR` to the exact frozen
  `M2_TCP_SECS=300` branch;
- M0/M1 refuse a TUN run that already contains M2 evidence;
- real-client evidence records and replays the actual Target, DNS, public,
  fake-IP, Exit, gateway, DNS-service, and IPv6 route state.

Code review has no unresolved P0/P1.

## Local Gates

```text
Knife15 runner internal self-test                 PASS
Knife15 external wrapper self-test                PASS
Knife14 low-RTT probe self-test                   PASS
Knife14 US-client suite self-test                 PASS
Knife14 sing-box control self-test                PASS
Bash syntax / git diff --check                    PASS
root all-targets + harness                        667 passed / 3 ignored
main                                                2 passed
integration harness                               10 passed / 4 ignored
cargo build --release                             PASS
cargo clippy --all-targets --features harness     PASS (existing warnings)
cargo fmt --all -- --check                        PASS
vendored Quinn                                    37 passed / 3 ignored
vendored Quinn docs                                1 passed
vendored quinn-proto                             309 passed
vendored quinn-proto docs                          3 passed
changed-content secret scan                       PASS
code review                                       no unresolved P0/P1
```

The internal self-test intentionally prints
`ERROR: command exceeded hard timeout of 1s` for its negative deadline case.
Its final PASS line and exit status zero are authoritative.

## Accepted Stop Position

Local implementation is sufficient for one fresh user-operated HK macOS M2.
The user must pull the pushed implementation, rebuild release, disable
Clash-TUN and every other VPN/TUN before baseline, and keep them disabled
through `stop`.

Run exactly:

```text
baseline -> direct-discriminator -> start -> smoke -> m2 -> status -> stop
```

Reserve about 25 wall-clock hours. On any start/smoke/M2 failure, preserve
`status -> snapshot -> stop`; do not tune constants or repeat unchanged before
the bundle is reviewed. M3 remains blocked until the real M2 bundle is
accepted.

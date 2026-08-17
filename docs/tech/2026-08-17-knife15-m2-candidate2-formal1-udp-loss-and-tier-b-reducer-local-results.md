# Knife15 M2 Candidate-2 Formal-1 UDP Failure And Tier-B Reducer Results

Date: 2026-08-17

## Outcome

Alibaba Tokyo candidate 2 produced a valid Tier-A quality failure in its fresh
Formal 1. The first `udp-reverse` phase completed with `6.164314%` receiver
loss, above the frozen `3%` limit. The paired evidence is complete, resource
and observer identity match, and cleanup passed. Candidate 2 is rejected; the
strict ledger emits `TIER_A_EXHAUSTED`. A third Tier-A resource or an unchanged
retry is not permitted.

Task 7 is also complete locally. The new Tier-B reducer implements the accepted
isolated-one-second frequency policy without changing strict `m2`, Rust
production code, the workload, transport, pool, QUIC, Endpoint, MTU, or any
frozen threshold. Task 8, the separate `m2-frequency` epoch action and ledger,
is next. M3 remains blocked.

## Immutable Formal Evidence

- source commit: `27a7ca0d67b9d2c76eb9c6fdb45304ab76edd539`
- Mac release SHA-256:
  `5e946af25ac05e74fa54d607fe4a67d7b22a1305c3b0a2e042fffb3d3cbf3032`
- runner SHA-256:
  `8b0d0c9ed220075481f4459dbfbde7d8ef88738d9ca6f2afc802b65d96f25a86`
- Mac bundle:
  `mini_vpn_knife15_macos_20260817_062054.tar.gz`
- Mac bundle SHA-256:
  `0501d259957b67cd2d49e284950063c35d8ba3162beeb75ce704cbc06503a116`
- Exit bundle:
  `mini_vpn_knife15_exit_target_observer_20260817_062143.tar.gz`
- Exit bundle SHA-256:
  `2453a153f394a5684668b4a6760e4931a927afd71dea82773d6cc12256d10d31`
- resource bundle SHA-256:
  `d0d25ec04d03c85c85ab0f096c39df2806c69072539dd88c070ebfd5817817d5`
- resource profile SHA-256:
  `2e7e7b0654ab7b4488ee6fe4cc1d79fa0f5ef0ac58e254a0ef060eac36adbe03`
- evidence root:
  `/Users/liushan/knife15-evidence/candidate2-alibaba-tokyo/27a7ca0d67b9d2c76eb9c6fdb45304ab76edd539/formal1-retry-20260817`

The run entered formal traffic at `2026-08-17T06:21:55Z`. TCP forward and
reverse completed. `udp-reverse` ran from `06:32:00Z` to `06:35:02Z`, returned
a complete valid iperf result, and failed on `6.1643143463% > 3%`. Receiver-zero
and sender-zero TCP intervals were both zero. Stop restored full-tunnel routes,
DNS, TUN/process ownership, and IPv6; `cleanup_evidence=PASS`.

## Failure Discriminator

The loss was not a local mini_vpn queue or Exit capture failure:

- the UDP result recorded `356,244` server-sent datagrams and `21,960` lost;
- loss was concentrated early and mid-phase, reaching roughly `11.4..14.2%`
  during the worst ten-second intervals before recovering below roughly
  `1.5%` late in the phase;
- mini_vpn reported zero UDP downlink drops, zero downlink backpressure, zero
  uplink drops, zero Endpoint socket-would-block, and zero interface errors;
- the Exit captured `3,740,335` packets with zero kernel drops;
- retained Exit TUIC egress stayed close to `19,660..19,710` packets per ten
  seconds with a maximum egress gap of only `69.730ms`;
- aligned Exit Target-ingress and TUIC-egress counters differed by only about
  14 packets across the failure window;
- the physical gateway probes stayed at zero loss, while Mac-to-Exit control
  probes twice reached `33.3%` loss during the same degraded period.

This selects transient loss on the HK-to-Tokyo public path after Exit kernel
egress, not Target TCP, Exit service, observer, TUN, D16, Endpoint accounting,
or a fixed bandwidth-capacity bottleneck. The TCP-pool remote-write diagnostic
occurred outside the UDP relay ownership path and is not causal for this UDP
quality failure.

## Tier-A Ledger Closure

The immutable attempt is sequence 4,
`quality_failure/udp_loss`. The ledger accepts the exact repaired runner bridge
and paired artifact integrity. `network_control_evidence=PARTIAL` reflects the
bounded three-packet Exit probe's missing RTT samples; it does not invalidate
the independently complete UDP result, resource admission, observer match, or
cleanup required for a genuine quality failure.

- attempt SHA-256:
  `8c61298f3ea2f86adfd3685294bc4f4f0d715e7180299b1fcf4c7ecd871ac5d7`
- ledger SHA-256:
  `96e50cd21bc9cc277596d82e90d4c7fc0a5856c24e05f94d58ab8b3dec7b1989`
- evaluation SHA-256:
  `8a95308964bb464fcaef1686eaebc7627839f269b174960365d0be4c0c2d8733`
- candidate 1: `rejected`, zero formal passes
- candidate 2: `rejected`, zero formal passes
- evaluation: `TIER_A_EXHAUSTED`

## Task-7 Tier-B Reducer

Created:

- `scripts/knife15-m2-frequency-summary.py`
- `scripts/fixtures/knife15-m2-frequency/`
- frequency reducer SHA-256:
  `a4920b97fa6b10f4c79986cf2f6cf1d012e5816bf2d460ecddf98d6db37cc782`
- reused market reducer SHA-256:
  `5ea5131f377a6b176985600f236949e19f47a9ceebd740c8fc3f8410d0271cc6`

The reducer imports the reviewed TCP parser from
`knife15-market-iperf-summary.py`; it does not invoke the canceled external
client comparison. Its public input is an exact collection of at most twelve
sealed six-hour epochs. Each epoch carries exact source, binary, runner,
resource profile, workload, server, observer, Exit, Target, UTC, monotonic,
evidence-segment, lifetime, and raw-result identity.

The enforced invariants are:

```text
max_consecutive_receiver_zero_intervals <= 1
receiver_zero_episodes_in_any_rolling_6h <= 1
receiver_zero_episodes_in_any_rolling_24h <= 3
receiver_zero_intervals_in_any_rolling_24h <= 3
```

Rolling windows are half-open: events exactly six or 24 hours apart are not in
the same window. UTC and monotonic reductions must agree. A new evidence
segment requires one exact positive invalid-infrastructure/evidence-unavailable
gap; a closed segment ID cannot be reused. Missing indices, clock reversal,
identity drift, an invalid epoch, missing interval, result overlap, path
traversal/symlink escape, or an unbounded input fails closed. A known evidence
gap prevents rolling-window bridging; it also cannot satisfy the separate
four-consecutive-epoch lifetime requirement.

Focused fixtures cover one isolated episode, three isolated episodes/day,
two consecutive zero intervals, two episodes inside six hours, four episodes
inside 24 hours, exact six-/24-hour boundaries, a final partial interval,
missing interval/epoch, clock reversal, identity mismatch, invalid epoch,
evidence-gap isolation, and closed-segment reuse.

## Local Gates And Review

Pass:

- `python3 scripts/knife15-m2-frequency-summary.py --self-test`
- `python3 scripts/knife15-market-iperf-summary.py --self-test`
- `python3 scripts/knife15-m2-continuity-ledger.py --self-test`
- Python byte compilation
- `git diff --check`
- focused path/provenance/secret review

Concentrated code review found and repaired one P1 before handoff: evidence
IDs were initially reusable as `A -> B -> A`, which could have rejoined two
closed windows. IDs are now single-use. It also closed client/server duration,
finite-number, timestamp agreement, symlink, and bounded-input gaps. There are
no unresolved P0/P1 findings.

## Next

Implement Task 8 with a separately named `m2-frequency` action and immutable
epoch ledger. Strict `m2` must remain unchanged and must reject Tier-B inputs.
Task 8 must admit only the exact `TIER_A_EXHAUSTED` ledger, seal twelve valid
six-hour epochs under one immutable identity, preserve complete epochs across
later infrastructure invalidation, require one uninterrupted four-epoch
lifetime, and retain all UDP, TCP-gap, DNS, real-client, ownership, observer,
safety, and cleanup gates. No Mac/VPS long run starts until the local runner,
ledger, review, and runbook gates pass.

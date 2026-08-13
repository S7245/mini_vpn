# Knife15 M2 Successor Service Floor Separation Formal Failure Results

Date: 2026-08-13

Status: **FORMAL M2 FAILED; ARCHITECTURE DISCRIMINATOR SELECTED; M3 BLOCKED**

Mac artifact:
`/tmp/mini_vpn_knife15_macos_20260813_053346.tar.gz` (SHA-256
`6aeddcf104aca085f7935c7e2c64791f30fd9c6df2fa9a0c8d57fe702821f074`).

Paired Exit observer:
`/tmp/mini_vpn_knife15_exit_target_observer_20260813_053436.tar.gz`
(SHA-256
`8a0c0c1941f4f1033479fd9299d6e9fe27bc925f36a9767b2f6e2500eeedee38`).

Exact source: `642e3aed30ccbcd98a71d4c359df0e86c961b5b2`.

## Verdict

The run passed baseline `23.289/52.799 Mbit/s`, a bounded 300-second direct
discriminator at `11.632 Mbit/s` with zero sender/receiver intervals, smoke,
all preflights, nine complete formal cycles, 87 phase results, nine DNS and
real-client checks, and cleanup. Maximum TCP gap was `8,650,752B`; maximum UDP
loss was `2.315939%`; all completed Target TCP results retained zero receiver
intervals.

Cycle 10 `short-reverse-4` did not create an iperf connection before the
unchanged hard bound. The result contained no connected stream or interval.
Formal M2 therefore remains failed even though the earlier traffic and final
cleanup were healthy.

## Selected Cause

The idle auxiliary connection retained the exact transport identity and path:

```text
stable identity / generation: 32934169616 / 2
current / proved path generation: 0 / 0
black-hole anchor / current: 0 / 0
historical certificate floor: 3,605,919B
current native Quinn cwnd:       381,502B
```

The certificate floor was the final result of a historical multi-round
handoff, not the fresh connection's minimum readiness proof. Admission
classified ordinary native congestion contraction as `stale_cwnd` and
repeatedly attempted to recreate `3,605,919B`. The failure-time successor lost
one `1,409B` packet in round 9, correctly failed closed, and had no qualified
same-open fallback. Admission returned `Saturated`; no TUIC Connect, business
payload, Exit-to-Target socket, or Target flow existed for the failed phase.

The paired observer captured `57,705,606` packets with zero kernel drops.
Target counters did not advance for the phase; TUIC counters saw only
successor-proof traffic. D16, TUN, Endpoint conservation, process, physical
interface, routes, DNS, Exit, Target, and cleanup were not the cause.

## Architecture Stop Rule

The result rejects a single persisted cwnd scalar serving both of these roles:

1. immutable newly authenticated successor readiness;
2. a transaction-only final proof that reaches one predecessor's current
   handoff requirement.

Do not retry the proof, tune cwnd/deadline/rates, enlarge the pool, or relax
qualified-only fallback. Preserve the first exact service turn as the
generation readiness certificate. Bind the later multi-round result only to
the current install transaction, and recompute every future handoff from
`max(readiness floor, exact current cwnd)`.

Architecture and plan:

- `docs/tech/2026-08-13-knife15-m2-successor-service-floor-separation-architecture-spec.md`
- `docs/tech/2026-08-13-knife15-m2-successor-service-floor-separation-implementation-plan.md`

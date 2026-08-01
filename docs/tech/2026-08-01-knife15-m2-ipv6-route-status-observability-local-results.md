# Knife15 M2 IPv6 Route-Status And Observability Local Results

Date: 2026-08-01

Status: **LOCAL PASS — exact pre-baseline check required before another TUN**

## Selected Failure

The second exact-source HK attempt used:

```text
bundle=/tmp/mini_vpn_knife15_macos_20260801_044032.tar.gz
sha256=651f977bd4756f0c4d25a086d583f6f58d5d1ba240e1f87ff59be6ae43ebf417
source_commit=19b5ceb1c04f4b8cb5b268f98df3fb5794998947
```

The user followed the documented `Automatic -> Off` service procedure, but
formal M2 again reported a physical IPv6-path error. The bundle proved that
source, start, smoke, Exit attribution, Endpoint conservation, and cleanup
were valid, but it did not contain the post-disable service state or the raw
IPv6 route result. M2 again remained `not_run`.

The exact formal command and probe reproduced locally after physical IPv6 was
absent:

```text
probe=2001:4860:4860::8888
route_status=0
route_text=route: writing to routing socket: not in table
old classification=unknown
```

The old runner recognized `not in table` only when `route` returned nonzero.
On this macOS version the same positive absence text can accompany status
zero, after which the old code tried to parse a missing `interface:` and
failed the physical-IPv6 gate. This is a runner false negative, not proof that
the service remained IPv6-enabled.

The first repaired runbook also checked a different Cloudflare IPv6 address
from the runner's Google IPv6 discriminator. Although both normally use the
same default route, a different probe cannot prove the exact formal
precondition.

## Repair Contract

One shared classifier now owns both the cheap public check and formal M2:

```text
exact known no-route line, no interface   -> safe_absent / none
status 0 + interface lo0 or utun*         -> safe_tunnel / exact interface
status 0 + any other interface            -> unsafe_physical / exact interface
nonzero unknown text or missing interface -> unknown / none
```

The known absence text is authoritative regardless of exit status. Unknown
text remains fail-closed; no generic nonzero result is accepted.

The new public action is read-only and does not require a TUN, baseline, TUIC
credential, route mutation, or DNS mutation:

```text
bash scripts/knife15-macos-soak.sh m2-ipv6-check
```

It prints the exact probe, route status, classification, interface, and raw
route text. The HITL runbook requires its PASS after the physical service says
`IPv6: Off` and before any baseline/direct work.

Formal M2 uses the same classifier and writes
`m2-ipv6-preflight.txt` before rejecting or continuing. `status` and the final
summary publish its classification/interface/status, so a future failure is
diagnosable from the immutable bundle without terminal stderr.

## Focused TDD And Feedback Loop

The first RED stopped because the new classifier did not exist. The selected
macOS behavior then had its own RED:

```text
ERROR: self-test: status-zero absent IPv6 route classification mismatch
```

The minimal GREEN recognizes the exact `not in table` line before exit-status
branching only when no interface is present. Focused fixtures cover:

- loopback and utun safe routes;
- physical-interface rejection;
- `not in table` with status one and status zero;
- ambiguous `not in table` plus physical-interface rejection;
- unknown nonzero text and status-zero output without an interface;
- persisted physical/safe evidence with route status and raw text;
- public help/wrapper visibility.

The original local reproduction now reports:

```text
route_status=0
classification=safe_absent
interface=none
route: writing to routing socket: not in table
PASS: M2 IPv6 route check
```

Internal Knife15 self-test, external wrapper self-test, Bash 3.2 syntax,
runbook Bash-block syntax, exact real-machine check, diff/link/secret hygiene,
and review gates pass. `shellcheck` is unavailable locally. No Rust data
plane, route scope, workload, SLO, frozen value, or IPv6-tunnelling non-goal
changed.

## Next Action

Do not run another baseline yet. Pull the repaired source on the HK Mac,
temporarily disable IPv6 for the exact physical service as documented, and
run only `m2-ipv6-check`. Continue to a fresh baseline/direct/start/smoke/M2
transaction only if it reports `safe_absent` or `safe_tunnel` and ends in
PASS. On failure, restore IPv6 immediately and send the structured output;
there is no reason to spend another baseline/direct/TUN cycle.

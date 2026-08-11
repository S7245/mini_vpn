# Knife15 M2 Successor Forward-Service Inheritance Formal Failure Results

Date: 2026-08-10 (artifact completed 2026-08-11 UTC)

Status: **FORMAL M2 FAILED; THE ONE-TURN SUCCESSOR SERVICE CERTIFICATE IS REJECTED AS SUFFICIENT; M2 AND M3 REMAIN BLOCKED**

Artifact:
`/tmp/mini_vpn_knife15_macos_20260811_014220.tar.gz`

SHA-256:
`2d8b4e6ee68177b8e3c9e2226e5717b8420e57b1ffb38fe59348dc9d84e2093e`

Exact source: `e531b3081661468eb08a5276c2c197a4aac78a23`

Production implementation under test: `300fb16`

## Provenance And Envelope

The archive hash matches. Its source, release binary, runner, configuration,
preflight, route/TUN ownership, and cleanup evidence are internally
consistent. The release binary SHA-256 is the same one used by the passing
paired qualification artifact from 2026-08-10.

Direct baseline passed at `12.838/54.648 Mbit/s`. The formal run completed five
whole cycles and, in cycle 6, the long forward, long reverse, reverse UDP,
`short-forward-1`, and `short-reverse-2` phases before failing
`short-forward-3`.

The exact failed result admitted `10,878,976B` at the sender with zero TCP
retransmits and delivered `4,194,304B` at the Target receiver. The first
complete Target receiver interval (`0..1.000731s`) contained zero bytes. One
sender-zero interval occurred later, but the formal failure discriminator is
the complete receiver-zero interval.

## Rejected Branches

- Exit and gateway probes had zero loss immediately before and at the failure;
  Exit RTT stayed about `164ms`.
- Interface errors and Endpoint socket would-block stayed zero.
- Endpoint byte conservation passed and finished at `61,414/0/0B` under a
  `61,440B` high-water mark.
- D16 accepted the failed stream's bytes and retained bounded ownership; its
  close was a zero-owned timed-transfer tail.
- The owning QUIC connection remained established on the same logical and path
  generation, with continuous ACK progress, zero black-hole additions, and a
  final `275,639B` cwnd.
- Process resources, Target route, DNS, TUN ownership, secret scan, and cleanup
  passed. The later Endpoint rebind attempt occurred after the workload had
  already failed and is not causal.

Therefore this is not an operator, bandwidth, VPS, Target, TUN, D16, Endpoint,
resource, cleanup, or frozen-parameter failure. No paired Exit observer was
provided for this formal run, so no packet-capture claim is made.

## Selected Causal Boundary

The cycle-6 `short-forward-1` stream first exercised the old auxiliary
generation. Its exact recovery reset recorded:

```text
cwnd_before=41,301B
cwnd_after=12,000B
```

Forward black-hole qualification then replaced it. The fresh successor passed
the existing single exact service turn and installed with a Ready certificate
at only `26,424B`. The intervening `short-reverse-2` used the same successor
and passed. `short-forward-3` then selected that same exact Ready
identity/path/certificate at `26,424B` and lost its first complete receiver
interval even though QUIC ACK progress continued.

This satisfies the previous architecture's explicit stop rule: a receiver-zero
interval recurred after a fresh certified generation owned the exact flow.
The one-current-cwnd service turn is therefore necessary transport evidence but
not sufficient forward-service inheritance. Repeating the same build or
tuning constants is prohibited.

## Decision

Deepen the existing generation-replacement transaction so an exact path reset
publishes the current generation's pre-reset forward-service floor. A successor
must complete one or more sequential, exact ACK-owned service turns within the
unchanged whole-replacement deadline until its observed cwnd reaches that
dynamic inherited floor. Loss, path change, no cwnd progress, close, or timeout
fails closed and preserves the existing bounded business fallback.

Architecture:
`docs/tech/2026-08-10-knife15-m2-successor-forward-service-inheritance-architecture-spec.md`.

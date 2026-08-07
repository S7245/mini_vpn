# Knife15 M2 Ordered-Gap Qualification Failure Results

Date: 2026-08-07

Status: **QUALIFICATION FAILED; ORDERED-GAP ACTIVE MIGRATION REJECTED**

Source: `f8c0639d1a3ae0f6a44dfba536f22512e9a60d3f`

Artifact:
`/tmp/mini_vpn_knife15_macos_20260807_031530.tar.gz`

SHA-256:
`60c60b7f365b681f018f5674d90fb6850615e91609d40b81be94489ca52492e6`

## Verdict

The bounded two-cycle qualification failed in cycle 1 `short-forward-1`.
More importantly, the new ordered-gap predicate issued six Endpoint rebinds
during the preceding `tcp-forward` phase even though that 300-second phase
completed successfully. The active-migration policy is therefore rejected as
unsafe without tuning its `250ms` observation interval.

The actual failed stream was an uplink data stream. It wrote `6,296,649B`,
waited up to `4,493,917us` for writer service, and received no business bytes.
An exact ordered receive gap cannot exist on that direction, so the rejected
predicate was also unreachable for the selected failure.

The Quinn read-only receive-progress seam remains valid mechanism. It proved
that a stable missing prefix can coexist with continuing multi-megabyte
out-of-order tail growth on a healthy WAN transfer. It must remain evidence,
not recovery authority.

## Provenance And Preflight

- binary SHA-256:
  `cee177aea4b503eaee80c182b27c2b0c04a5bc8e21b4fefdaf8672b8a1a6d9b0`;
- runner SHA-256:
  `6ef6ca404423dfca06ef7f5473b641146eb5f4a73bff6dd664c97270cc8e0442`;
- baseline forward/reverse receiver:
  `24.026433 / 58.983805 Mbit/s`;
- fresh 300-second direct discriminator passed with its exact manifest and
  result hashes;
- start, smoke, IPv6, full-tunnel, real-client, and controlled-drain
  preflights passed;
- cycle 1 long forward, reverse TCP, and reverse UDP completed;
- reverse UDP loss was `2.072342%`, below the unchanged `3%` limit;
- stop restored routes and DNS, stopped the process, and finalized cleanup.

Formal M2 was not run.

## Ordered-Gap False Positives

All six actions targeted conn1 stable identity `40849162256`, reader `2`,
stream `1`. Each became eligible after exactly two samples and `250..251ms`:

| Episode | Read offset | Gap | Highest received | Buffered tail |
|---:|---:|---:|---:|---:|
| 1 | 5,417,644 | 6,930B | 10,383,058 | 2,343,615B |
| 5 | 14,549,185 | 8,709B | 22,059,376 | 6,039,150B |
| 16 | 37,047,487 | 1,386B | 45,427,779 | 8,113,823B |
| 39 | 87,954,322 | 9,702B | 94,449,217 | 4,358,982B |
| 45 | 100,193,728 | 1,386B | 108,394,032 | 8,342,794B |
| 47 | 109,618,294 | 8,316B | 117,071,266 | 5,436,217B |

The phase later completed `119,087,605B`. Stable `read_offset` and
`next_received_offset` for one sampler interval did not imply that the QUIC
receive path had stopped; the tail continued accumulating far beyond each
small missing prefix. Six source-port migrations on this passing stream are
false positives regardless of whether they contributed to the later failure.

## Selected Failure

Cycle 1 `short-forward-1` offered `19.221146 Mbit/s` for ten seconds:

- sender admitted `9,830,400B` at `7.857 Mbit/s` with zero TCP retransmits;
- Target received `4,325,376B` at `3.401 Mbit/s`;
- sender had four complete zero intervals;
- Target receiver had two complete initial zero intervals;
- conn0 stable identity `4385938992`, stream `5`, owned the data flow;
- its writer accepted `6,296,649B` and waited up to `4,493,917us`;
- no exact writer ACK-stall rebind or connection-local path reset fired;
- no ordered read progress existed on the uplink data direction.

The current artifact records only the writer's final maximum wait and action
threshold outcomes. It does not preserve the ACK trajectory across that exact
Pending episode, so it cannot distinguish continuous low-rate QUIC ACK service
from an initial-payload QUIC supply outage. That is the next observation seam.

## System Discriminators

- Endpoint conservation max/final was `61,440B / 61,414/0/0B`;
- Endpoint socket would-block and interface errors remained zero;
- D16 closed with zero queued, leased, and reserved ownership;
- gateway and Exit controls, routes, process, TUN, and cleanup remained valid;
- conn0 later reported only `2,818B` loss, two congestion events, and zero
  PLPMTUD black holes;
- the two `Stopped(0)` writes occurred at timed-transfer close tails and do
  not establish the active failure cause.

The evidence rejects operator error, slow baseline, physical-path continuity,
TUN/D16/Endpoint capacity, cleanup, and frozen-parameter tuning branches.

## Stop-Rule Outcome

Both qualification stop rules fired:

1. the new recovery action changed a healthy passing flow, so it is unsafe;
2. the selected receiver-zero failure had no exact ordered receive gap, so the
   architecture was not sufficient for the observed failure.

Do not tune the observation interval or broaden TCP silence. Remove the active
ordered-gap migration authority, retain read-only evidence, add bounded exact
writer/ACK episode evidence, and run one short differential qualification
with the Exit observer before considering another recovery mechanism.

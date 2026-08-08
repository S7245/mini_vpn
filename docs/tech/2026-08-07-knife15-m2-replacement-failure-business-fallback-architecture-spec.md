# Knife15 M2 Replacement-Failure Business Fallback Architecture Spec

Date: 2026-08-07

Status: **ACCEPTED AND IMPLEMENTED LOCALLY; ONE MAC QUALIFICATION REQUIRED**

Failure source:
`docs/tech/2026-08-07-knife15-m2-successor-service-turn-qualification-failure-results.md`.

## Goal

Prevent a failed auxiliary-generation maintenance replacement from failing the
business open that exposed the degraded slot when another already-qualified
current generation can serve that open.

The change is intended to be sufficient for the exact discriminator where a
terminal successor service-turn loss currently becomes
`remote_open handshake_failed` and leaves iperf unconnected. It is not formal
M2 acceptance, a throughput feature, or a general transport retry.

## Non-Goals And Frozen State

- do not weaken successor service-turn ACK/loss/path/close/deadline rules;
- do not install or reuse a failed successor;
- do not replay a TUIC Connect or any business payload;
- do not retry replacement within the same business open;
- do not expand the pool or eligible-generation count;
- do not change D16, MTU/PLPMTUD, QUIC windows, chunking, Cubic, GSO, Endpoint
  rate/burst/fairness, self-wake, recovery bounds, workload, or SLO;
- do not add a Target probe, timer, threshold, score, or configuration knob.

## State Transition

Each call to `acquire_tcp_pool_reservation` owns bounded attempt-local state:

```text
failed_replacement_slots[pool_len]
failed_replacement_error: Option<(slot, error)>
```

The transition is:

```text
normal admission
  -> ReplaceAuxiliary(slot)
  -> authenticate + exact successor service turn + existing CAS
     -> success: install and reserve successor (unchanged)
     -> failure:
          keep predecessor current
          mark slot excluded for this open
          resample admission in qualified-only mode
            -> qualified current generation: reserve it
            -> preparing qualified generation: wait on existing Notify
            -> no qualified generation: fail closed with both errors
```

Attempt-local exclusion disappears when the business-open future returns. A
later independent open therefore gets fresh replacement authority.

## Invariants

1. Service-turn failure never installs the successor.
2. The predecessor remains current and no new draining generation appears.
3. The failed slot cannot be selected through normal or all-degraded fallback
   for the same open.
4. After one replacement failure, every non-qualified candidate is ineligible;
   therefore the same open cannot replace another auxiliary slot.
5. A fallback reservation uses the existing atomic preparation flag,
   generation activity CAS, lease, total ownership, and drop accounting.
6. `Notify::notified()` is created before admission, preserving the existing
   no-missed-wake ordering.
7. No safe fallback preserves the original replacement cause and adds the
   exact terminal admission cause.
8. No payload, credential, or secret enters diagnostics.

## Boundedness And Capacity

The only new state is one boolean per configured pool slot and one bounded
error string for the single failed replacement. Pool length is already
bounded by existing configuration. There is no queue, background task, new
timer, new socket, or retained generation.

This change is outside the established data byte hot path. The frozen Endpoint
candidate remains:

```text
rate:             30,720,000 wire B/s
burst:                61,440B
1ms bound:             92,160B
10ms bound:           368,640B
application capacity: about 239.167 Mbit/s
```

The exact 32 MiB loopback gate must remain above `170 Mbit/s`, deliver exact
bytes and clean EOF, preserve
`available + live + outstanding <= 61,440B`, and finish with zero live and
outstanding debt.

## Observability

One failure line records failed slot, exact existing error, and
`same_open_retry=false`. A successful business fallback records failed and
selected slots. If no qualified current generation exists, the returned error
contains both maintenance and admission causes.

The Mac discriminator must show the service-turn failure, no successor install
for that attempt, and either the same open selecting another qualified current
generation or an exact fail-closed no-fallback result. A later open may
legitimately initiate one fresh replacement.

## TDD And Acceptance

A deterministic three-slot replay must prove:

- the first degraded auxiliary requests replacement;
- after simulated failure, that slot is excluded;
- another degraded auxiliary is not granted a second same-open replacement;
- the qualified current generation owns exactly one fallback lease;
- a concurrent admission observes `Busy` while its preparation is held;
- lease drop restores exact total ownership;
- a later independent open again has replacement authority.

Local acceptance additionally requires complete root/binary/integration,
release, established Clippy, shell, vendored Quinn/proto/docs, formatting,
diff, vendor, secret, and exact Endpoint-capacity gates, followed by review
with no unresolved P0/P1.

Mac acceptance is exactly one paired-observer
`m2-ipv6-check -> baseline -> direct-discriminator -> start -> smoke ->
m2-qualification -> status -> stop`. Qualification can only produce
`PASS_NON_ACCEPTANCE`; formal M2 and M3 remain blocked.

## Stop Rule

Reject the architecture without tuning if the same business open performs a
second replacement, selects a degraded/unknown fallback, changes a frozen
value, leaks ownership, reduces the exact 32 MiB gate to `<=170 Mbit/s`, or
reproduces the unconnected reverse-open discriminator after the fallback is
observed.

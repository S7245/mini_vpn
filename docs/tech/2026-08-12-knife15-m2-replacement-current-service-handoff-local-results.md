# Knife15 M2 Replacement Current-Service Handoff Local Results

Date: 2026-08-12

Status: **LOCAL TDD, CAPACITY, AND REVIEW PASS; PAIRED M2 QUALIFICATION
REQUIRED; FORMAL M2 AND M3 BLOCKED**

Formal failure:
`docs/tech/2026-08-12-knife15-m2-replacement-current-service-handoff-formal-failure-results.md`.

Architecture:
`docs/tech/2026-08-12-knife15-m2-replacement-current-service-handoff-architecture-spec.md`.

## Implemented Contract

Direct auxiliary replacement now begins with one slot-owned current-service
handoff transaction. While holding the generation-slot mutex it verifies the
exact stable transport identity and logical generation, samples the current
Quinn cwnd, rejects zero, and publishes:

```text
required_floor = max(previous_owned_floor, current_generation_cwnd)
```

The successor then uses the existing exact sequential ACK-owned service proof
to reach the required floor under the unchanged five-second whole-replacement
deadline. The existing install CAS rechecks the latest generation-owned floor
before ownership transfer. Stale identity, zero cwnd, loss, path change, no
progress, timeout, or insufficient proof fail closed; the current predecessor
and existing qualified fallback remain available.

The replacement-start diagnostic now records `observed_current_cwnd`
separately from `inherited_forward_service_floor`. No public API or static
service parameter was added.

## TDD Result

The focused RED failed with the expected missing handoff interface. GREEN uses
a real local Quinn pair: an exact predecessor first owns an older positive
floor, grows its current cwnd through ACK-owned service, and then proves that
replacement preparation returns and publishes the exact higher current value.

Focused coverage also proves:

- a stale logical generation cannot read or publish handoff state;
- the generation-owned floor is monotonic and later lower evidence cannot
  weaken it;
- the successor proof dynamically performs multiple exact service turns;
- install CAS rejects proof below the latest owned floor;
- current and draining path-reset ownership stays atomic.

## Gate Results

- root library: `707 passed; 3 ignored`;
- root binary: `2 passed`;
- integration: `10 passed; 4 ignored`;
- release build and release full-TUN/capacity discriminator: PASS;
- established root and vendored Quinn/quinn-proto Clippy lanes: PASS;
- vendored Quinn: `40 passed; 3 ignored`, docs `1 passed`;
- vendored quinn-proto: `330 passed`, docs `3 passed`;
- root docs, rustfmt, shell syntax, macOS runner self-test, Exit observer
  self-test, diff/provenance, and secret checks: PASS.

The exact release 32MiB Endpoint capacity result was `239.536 Mbit/s`, above
the frozen `170 Mbit/s` architecture stop boundary. Endpoint final
available/live/outstanding ownership was `61,440/0/0B`; socket would-block was
zero.

The complete paired Exit artifact was copied independently and verified before
analysis:

```text
/tmp/mini_vpn_knife15_exit_target_observer_20260812_035039.verified.tar.gz
SHA-256 643a019a9e1122f26672b527c19464ea4c6a9384169b87145fd8396f0a65f36c
```

Code review found no unresolved P0/P1 across identity ownership, mutex scope,
bounded proof work, install CAS, failure fallback, predecessor drain,
TCP/UDP/TUN/D16/Endpoint regression, diagnostics, or test coverage.

## Decision And Next Gate

The implementation is locally ready only for one fresh paired
`m2-qualification`. Do not run formal M2, repeat `85d8772`, tune constants, or
relax the receiver-zero SLI.

Run:

```text
m2-ipv6-check -> baseline -> direct-discriminator -> start -> smoke
-> fresh .33 observer start -> m2-qualification -> status -> stop
```

The observer must be fresh, healthy, matching, and started after smoke. Sync
both Mac and Exit bundles. A Target receiver-zero after the successor proves
the exact handoff floor rejects this mechanism without repetition or tuning.
A clean qualification only reopens formal M2; M3 remains blocked until formal
M2 and cleanup pass.

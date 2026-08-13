# Knife15 M2 ACK-Progress Path Reset Retirement Local Results

Date: 2026-08-12

Status: **LOCAL TDD, CAPACITY, AND REVIEW PASS; PAIRED M2 QUALIFICATION
REQUIRED; FORMAL M2 AND M3 BLOCKED**

Formal failure:
`docs/tech/2026-08-12-knife15-m2-ack-progress-path-reset-formal-failure-results.md`.

Architecture:
`docs/tech/2026-08-12-knife15-m2-ack-progress-path-reset-retirement-architecture-spec.md`.

## Implemented Contract

mini_vpn no longer grants the Endpoint recovery monitor authority to emit or
execute `TcpPathDegraded -> ResetConnectionPath -> Connection::path_changed()`.
The removed surface includes the black-hole anchor/consumption policy state,
action and trigger variants, monitor executor, stable-id-to-slot handle copy,
slot reset adapter, outcome/snapshot types, and obsolete positive tests.

Exact ACK-stall and UDP no-RX Endpoint rebind remain unchanged. Auxiliary
generation replacement still atomically publishes the exact current service
floor, proves the successor through ACK-owned turns, transfers future opens by
CAS, and drains existing predecessor streams without migration or replay.
Native Quinn loss recovery, Cubic, and PLPMTUD retain current-flow ownership.

No D16, MTU, pool, QUIC window, chunk, Cubic, GSO, Endpoint pacing, self-wake,
workload, recovery-bound, or SLI value changed. `src/` contains no remaining
`path_changed()` call.

## TDD Result

The focused RED supplied one stable connection whose writer had remained
`Pending` for the existing two-second bound, had acknowledged `65,536B`, and
advanced its black-hole counter `0 -> 1`. It failed exactly because the old
policy returned `ResetConnectionPath` instead of `None`.

GREEN removes the rejected authority. Focused coverage now proves:

- ACK progress plus one black-hole increment is non-destructive;
- later ACK growth `65,536 -> 131,072B` plus another increment is also
  non-destructive;
- exact ACK stall still selects `TcpWriteStall -> Rebind`;
- an in-flight Endpoint rebind remains the sole recovery owner;
- replacement handoff, successor proof, monotonic floor, install CAS, and
  predecessor drain remain intact.

The first unqualified `--exact` command ran zero tests and was rejected. The
real RED and all GREEN commands used counted filters/module paths.

## Gate Results

- root library: `705 passed; 3 ignored`;
- root binary: `2 passed`;
- integration with `harness`: `10 passed; 4 ignored`;
- focused Endpoint recovery: `15 passed`;
- replacement handoff: `2 passed`; successor: `9 passed`; generation: `4
  passed`;
- release build and release full-TUN capacity/EOF discriminator: PASS;
- established warning-tolerant all-target `harness` Clippy: PASS with only
  the known repository baseline warnings;
- vendored Quinn: `40 passed; 3 ignored`, docs `1 passed`, default Clippy
  PASS with the exact local proto patch;
- vendored quinn-proto: `330 passed`, docs `3 passed`, default Clippy PASS;
- root docs, rustfmt, shell syntax, macOS runner self-test, Exit observer
  self-test, diff, local-proto provenance, and changed-secret checks: PASS.

The strict exploratory `-D warnings` command promoted 17 existing lints in
untouched code to errors. Per the established project gate this result was
rejected, recorded, and replaced by the documented warning-tolerant lane; no
new warning came from the path-reset retirement diff.

The exact release 32MiB Endpoint gate reached `240.079 Mbit/s`, above the
frozen `170 Mbit/s` architecture stop boundary. Endpoint terminal
available/live/outstanding ownership was `61,440/0/0B`, socket would-block was
zero, and the full-TUN batch discriminator reported zero ring drops/full
waits.

Code review found no unresolved P0/P1 across recovery authority, ACK
semantics, current-flow ownership, generation replacement, predecessor drain,
TCP/UDP/TUN/D16/Endpoint regression, cleanup, diagnostics, or test coverage.

## Decision And Next Gate

The formal artifact hit the old architecture's explicit stop rule, so neither
the failed source nor the retired path reset may be repeated or tuned. This
implementation is locally ready only for one fresh paired
`m2-qualification`:

```text
m2-ipv6-check -> baseline -> direct-discriminator -> start -> smoke
-> fresh .33 observer start -> m2-qualification -> status -> stop
-> observer freeze/bundle
```

A Target receiver-zero with local controls healthy rejects the new native-
recovery ownership result without repetition or parameter changes. A clean
qualification only reopens one fresh formal M2; M3 remains blocked until
formal M2 and cleanup pass.


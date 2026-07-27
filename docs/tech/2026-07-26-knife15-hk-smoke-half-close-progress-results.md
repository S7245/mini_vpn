# Knife15 HK Smoke Half-Close Progress Results

Date: 2026-07-26

Status: **Root cause accepted and local repair pushed at `44ff086` — fresh
user-run HK diagnostic sequence pending**

## Outcome

The user-operated HK archive
`/tmp/mini_vpn_knife15_macos_20260724_105822.tar.gz` (SHA-256
`b5ce3af428d0359cad2f7e6089d389da791c9545e8c033c5723b869ac7356f36`)
did not run M1 diagnostic. It completed both smoke workloads and fake-IP DNS,
then failed the existing TCP-pool idle safety barrier because one reverse-data
relay retained a native pool lease.

This is a real local close-lifecycle defect in exact source `a3ebe42`, not an
operator error, an M1 data-quality result, or permission to tune a frozen
value. Commit `44ff086` repairs the selected boundary without changing the
runner, workload, SLI, D16 capacity, MTU, pool, QUIC windows, chunk size,
Cubic, GSO, Endpoint pacing, or self-wake behavior.

## Provenance And Operation

The supplied checksum matches. The archive contains no absolute path, parent
traversal, link, or special-device entry. Its manifest records:

```text
source  a3ebe42d6445644aed2c5a28cf8e0202aaaf7d5f
binary  05425026fcd17502f4e8205e187b881df71607d55b41623b02ee64da3f81d5c0
runner  480338657cccbe6590626c574e0ac5923e175996f0f68ff9bcfab662777af6a2
```

These identities match the reviewed source and artifacts. The event sequence
was correct and bounded:

```text
10:58:22Z start requested
10:58:23Z ready utun=utun4
10:58:25Z smoke start
10:59:58Z smoke TCP pool drain failed
10:59:59Z stop requested
11:00:00Z stop cleanup complete
```

The user correctly preserved evidence and stopped after the safety failure.
M1 and M1 diagnostic remained `not_run`.

## Selected Failure

Forward and reverse TCP commands completed, fake-IP DNS passed, and the
reverse receiver had no zero interval:

```text
direction  sent bytes   received bytes   sent rate       received rate
forward     43,384,832       35,651,584   17.351565 Mb/s  14.142494 Mb/s
reverse    130,809,856      123,207,680   51.874975 Mb/s  49.270528 Mb/s
```

After reverse application completion, data handle 1 epoch 3 entered
`Established -> CloseWait`. Its control peer drained, but the data relay kept
exactly `524,288B` queued, zero leased/reserved bytes, and one pool lease.
Four consecutive 10-second observations were identical:

```text
payload_owned_bytes=524288
queue_queued=524288
queue_leased=0
queue_reserved=0
queue_closed=false
```

Each observation unconditionally re-armed the half-close timer. The actor
then executed roughly 19,000 additional no-progress egress windows without
another admitted byte or `send_slice` advance. The pool could therefore never
reach the exact zero required by smoke.

The immediately preceding successful smoke bundle released the same bounded
`524,288B + 27,840B` close tail through dead-slot reap. The failed bundle
instead retained a graceful `CloseWait` slot, exposing the distinction between
owned-byte presence and owned-byte progress.

## Rejected Branches

- Endpoint conservation remained at or below `61,440B` and ended
  `61,414/0/0B`.
- Pump-full waits, pump read errors, TUN flush failures, interface errors,
  abandoned Endpoint bytes, and log compactions were zero.
- Three Endpoint socket rebinds all recovered current-generation traffic in
  `249–252ms`; rebind could not discharge this business-stream lifecycle.
- One `1/3` Exit ICMP sample recovered, while every gateway sample was
  lossless. Path quality cannot explain an indefinitely static local lease.
- Process, TUN, and owned routes cleaned after `stop`.

These invariants reject operator procedure, pacing conservation, TUN drain,
resource leakage, and a failed socket-rebind implementation as the selected
cause.

## Repair And TDD

The byte-owned queue now exposes monotonic `local_progress_bytes` from the
module that owns both progress transitions:

- queued payload becomes a lease;
- leased permit bytes are released toward local egress.

The relay preserves the first 10-second half-close deadline containing useful
D16 ownership. A later deadline re-arms only if that monotonic counter
advanced since the preceding owned window. Static ownership then reaches the
existing `half_closed_idle_timeout` path instead of vetoing cleanup forever.
Slow but progressing payload remains protected; a queued-to-leased move that
later stalls earns one additional bounded window.

A paused-time RED test reproduced unchanged queued ownership surviving
forever. It failed before the repair and now proves first-window preservation
followed by bounded termination. The existing moving-ownership test still
proves that real queued/leased progress continues to defer close. Queue tests
also lock the monotonic counter at push, lease, partial release, and full
release boundaries.

## Local Gates

```text
focused half-close tests                           2 passed
tcp_downlink_pump module                          24 passed
root all-targets + harness                       667 passed / 3 ignored
main                                                2 passed
integration harness                               10 passed / 4 ignored
real Quinn 32 MiB capacity/conservation gates     PASS (>170 Mbit/s)
cargo build --release                             PASS
cargo clippy --all-targets --features harness     PASS (existing warnings)
Knife15 runner + wrapper self-tests               PASS
Knife14 shell self-tests and shell syntax         PASS
vendored Quinn                                    37 passed / 3 ignored
vendored Quinn docs                                1 passed
vendored quinn-proto                             309 passed
vendored quinn-proto docs                          3 passed
cargo fmt / git diff / changed-secret scan        PASS
code review                                       no unresolved P0/P1
```

The Knife15 self-test intentionally emits its negative one-second hard-timeout
line before its final PASS line.

## Decision And Next Action

Because source and release binary must change, the previous baseline/direct
artifacts cannot establish provenance for this repair. Pull `44ff086`, rebuild
release, and take one fresh user-operated HK sequence:

```text
baseline -> direct-discriminator -> start -> smoke -> m1-diagnostic
-> status -> stop
```

Proceed to `m1-diagnostic` only after baseline, direct, and the repaired smoke
pool-idle barrier pass. Keep Clash-TUN and every other VPN/TUN disabled from
baseline through `stop`. On any safety failure preserve
`status/snapshot/stop`.

The diagnostic artifact remains longitudinal evidence only. It cannot accept
M1 or unblock M2/M3; formal M1 still requires a separate clean `m1` run.

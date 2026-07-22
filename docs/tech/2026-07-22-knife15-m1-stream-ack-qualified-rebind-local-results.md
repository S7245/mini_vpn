# Knife15 M1 stream-ACK-qualified Endpoint rebind local results

Date: 2026-07-22

## Result

Exact-source `e479013` bundle
`/tmp/mini_vpn_knife15_macos_20260722_094608.tar.gz` (SHA-256
`7443769e1ebc61cae241c6cda221dda1a5221909a23133c1d3e1103c6b023313`)
was operated correctly but failed M1 cycle 1 forward. Fresh baseline was
`31.257906/26.416944 Mbit/s`; the 300-second direct prerequisite passed at
`15.621349 Mbit/s` with no receiver-zero interval.

M1 offered `15.628953 Mbit/s` and delivered the exact phase bytes, but the
Target receiver had three complete zero-byte seconds (10, 12, and 14) and the
local sender had 24 zero intervals. The Endpoint issued seven successful
`tcp_write_stall` rebinds. The first two arose during smoke; generations 3--7
repeatedly changed source port while the M1 business writer remained blocked.
Connection-level current-socket RX declared each migration recovered in
`249--500ms`, but did not prove progress for the blocked business stream.

The direct path, physical interface, routes, TUN, resources, Endpoint
conservation, and cleanup were healthy. This rejects operator error, stale
source, baseline discontinuity, and a TUN/ownership failure. The selected root
is the previous repair's over-broad trigger: ordinary Pending plus continuing
business ACK progress was classified as a path black hole, producing a
positive-feedback rebind/congestion loop.

## Repair

Vendored quinn-proto now reports monotonic written and acknowledged bytes for
one send stream. Vendored Quinn exposes a cloneable read-only handle for that
stream. Every generic and native/D16 TUIC writer installs the matching handle
in per-writer pressure state.

The existing 250ms recovery monitor samples all currently Pending writers.
ACK advancement on the exact stream resets only its ACK-stall clock. A TCP-
write recovery now requires both continuous Pending and no exact-stream ACK
progress for the unchanged `clamp(8 * max_rtt, 2s, 7s)` bound. Other-stream
ACKs cannot mask a stall. One rebind covers all writer episodes present in the
sample, and the accepted no-RX plus authenticated set-wise current-generation
recovery paths remain unchanged.

Logs preserve `tuic-endpoint-rebind` and add `write_writer`, `write_stream`,
`write_acknowledged`, and `write_pending_ms`. No frozen D16, MTU, UDP payload,
pool, QUIC window, chunk, Cubic, GSO, self-wake, pacing, recovery, workload, or
SLO value changed.

## TDD and review

The first focused RED proved that a Pending writer still reported `2.25s` of
stall after acknowledged bytes advanced at `2s`; the expected value was
`250ms`. The completed tests prove:

- exact-stream ACK progress resets only ACK-stall age and suppresses rebind;
- a real ACK-stalled Pending writer still triggers recovery despite unrelated
  connection RX;
- one rebind covers multiple writers on one connection and across the pool;
- cleared/new writer episodes rearm correctly;
- Quinn's real loopback handle observes application writes and peer ACKs;
- quinn-proto progress remains monotonic across flow-control and ACK paths;
- existing no-RX, migration, authentication, Endpoint conservation, D16, TUN,
  TCP, UDP, fake-IP DNS, and runner behavior remains green.

Review checked lock ordering, poisoned-lock recovery, atomics and publication,
writer ID/episode lifecycle, weak-registry cleanup, stream close/sample failure,
generic/native/D16 reachability, per-stream isolation, rebind de-duplication,
old-socket retention, log compatibility, and frozen-value drift. The initial
test-only adapter constructor warning was removed from production builds. No
unresolved P0/P1 remains.

## Final local gates

```text
focused Endpoint recovery       8/8
focused TCP write pressure      3/3
root all-targets                653 passed / 3 ignored; main 2/2
32 MiB EndpointWindowV1         240.472 Mbit/s; final 61,440/0/0B
vendored Quinn                  37 passed / 3 ignored; doc 1/1
Quinn integration               1 expected ignored
vendored quinn-proto            309/309; doc 3/3
cargo build --release           PASS
cargo clippy --all-targets      PASS (established warnings only)
Knife15 runner self-test        PASS
Knife15 wrapper self-test       PASS
Knife14 three shell self-tests  PASS
shell syntax / root fmt / diff  PASS
changed-content secret scan     PASS
```

The Knife15 runner's printed one-second timeout error is its expected negative
fixture and exited successfully. Standalone Quinn used the required absolute
local quinn-proto patch and executed a nonzero test count.

## Accepted stop position

The false-rebind architecture is locally repaired and the exact capacity gate
remains above `170 Mbit/s`, but the failed partial bundle is not an eight-hour
M1 acceptance. Next take one fresh user-operated HK M1 from the pushed repair,
with a rebuilt release and fresh baseline/direct artifacts. Disable Clash-TUN
and every other VPN/TUN before baseline and keep them disabled through Knife15
`stop`. On failure preserve `status -> snapshot -> stop`. Do not tune frozen
values or waive TCP/UDP SLIs. M2 and M3 remain blocked until a complete M1
passes.

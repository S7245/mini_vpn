# Knife15 HK macOS M0 Rearm Acceptance Results

Date: 2026-07-17

Status: **PASS — independent re-create/rearm/cleanup accepted; Knife15 M0 is
complete and M1 planning is unblocked**

## Provenance

- Bundle: `/tmp/mini_vpn_knife15_macos_20260717_063948.tar.gz`
- Bundle SHA-256:
  `f4e0f649bf9d79f5e39f8991b2c76e55641880b5bcc5115e44654bef24168d8a`
- Source: `d3f7b13299d323b7edbad75183dba917afc97b3f`
- Release binary SHA-256:
  `b895854bb67205c406e7d1e3d1d032dd8beae2f94e2c1d8a652e2d6196d7ae8b`
- Runner SHA-256:
  `9d8c69e213a0513aab1adff7acd38e4f488cb4d588ad543d3b79813f8f975eb6`
- Host: HK arm64 Mac, macOS `26.5.1`

The source commit differs from the main M0 source `8bc7b7c` only by the M0
results, handoff, plan, and learning documents. The release binary and runner
hashes are identical to the accepted main run, so the executable system under
test is unchanged.

## Fresh Create And Smoke

The rearm started independently at 06:39:48 UTC, created fresh `utun4`, and
kept the TUIC Exit on physical `en1`. Target and DNS routes moved to `utun4`.
The smoke ran from 06:39:51 to 06:40:34 and completed:

- forward TCP: `39,714,816B` sent and `32,768,000B` acknowledged by the Target
  receiver (`12.996259 Mbit/s` receiver aggregate);
- reverse TCP: `125,566,976B` received locally (`50.223192 Mbit/s`) with all
  20 local receiver intervals positive;
- fake-IP DNS: `example.com A -> 198.18.0.2` through `8.8.8.8`.

The summary's `m0_status=not_run` is required for this scoped rearm: section 5
of the runbook explicitly forbids repeating the two-hour workload.

Four early zero-byte rows in the forward client JSON are local sender samples,
not Target receiver evidence. The Target receiver aggregate completed and this
short rearm smoke is a lifecycle gate rather than the formal M0 receiver SLI;
the accepted main run already supplied 74 direction-aware result files with
zero receiver-zero intervals.

## Ownership, Review, And Cleanup

- Endpoint conservation passed with maximum `61,365B`.
- Final Endpoint state was `61,277/0/0B` available/live/outstanding; abandoned
  bytes, would-block events, and stale wakers were zero.
- No endpoint rebind was attempted or needed.
- TUN and physical interface error samples were zero; all five network control
  samples were complete and the physical gateway had zero loss.
- The summary retained one canonical forward Quinn `Stopped(0)` close tail.
  Its D16 queued/leased/reserved ownership was `0/0/0` and it had no terminal
  pending, send, or flush error.
- The reverse application-first close released the exact accepted bounded
  `524,288B` D16 reservoir plus `27,840B` terminal local send queue. It ended
  with no D16 queue ownership and is not stranded data.
- Logs did not compact and the secret scan passed.
- Stop completed at 06:40:35. The final process row was dead, the final TUN
  interface row was unavailable, and Target, Exit, and physical routes all
  resolved to `en1`.

The close tails are the same phase- and ownership-classified REVIEW events
already accepted in the main M0 analysis. Review found no unresolved P0/P1.

## Decision

The independent fresh `start -> smoke -> stop` gate passes. Together with the
accepted two-hour main bundle, Knife15 M0 is complete. No M0 repeat, baseline,
direct discriminator, runner repair, or frozen-parameter change is required.

M1 may now enter its local design/TDD stage. Before an eight-hour macOS run,
define explicit M1 resource plateau, receiver continuity, UDP loss, recovery,
network-control, ownership, log, and cleanup SLOs from the observed M0 envelope
and extend the runner under deterministic shell tests. M1 execution must not
reuse the two-hour command as an unreviewed longer loop.

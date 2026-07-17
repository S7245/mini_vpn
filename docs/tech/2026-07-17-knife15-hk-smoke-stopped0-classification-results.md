# Knife15 HK Smoke `Stopped(0)` Classification Results

Date: 2026-07-17

Status: **SCOPED SMOKE PASS; MANUAL STOP GATE CORRECTED; FRESH DIRECT/M0
PENDING**

## Evidence And Provenance

- bundle: `/tmp/mini_vpn_knife15_macos_20260717_033923.tar.gz`;
- SHA-256:
  `a189b848cc9d685152573aefb0141261bee9f6b6b5092c03786987acc1967ec5`;
- source: `13faccc13e944ac1f64d61312943f7f565d7bf88`;
- runner SHA-256:
  `9d8c69e213a0513aab1adff7acd38e4f488cb4d588ad543d3b79813f8f975eb6`;
- binary SHA-256:
  `b895854bb67205c406e7d1e3d1d032dd8beae2f94e2c1d8a652e2d6196d7ae8b`.

The bundle checksum matches the user-provided value. Target, Exit, and DNS
began on physical `en1`; start captured `DNS_TARGET=8.8.8.8` and routed Target
and DNS through the new `utun4` while keeping the Exit outside it.

## Smoke Result

Forward and reverse iperf commands completed without JSON errors. Forward sent
`25,821,184B` and reported `19,267,584B` received at `7.639 Mbit/s`. Its seven
zero client intervals are sender/backpressure diagnostics, not the formal
Target receiver SLI. Reverse local receiver delivered `128,057,344B` at
`51.212 Mbit/s`. DNS through `8.8.8.8` returned the expected fake IP
`198.18.0.2` for `example.com`.

Endpoint conservation passed with a maximum of `61,414B` and ended at
`61,388/0/0B` available/live/outstanding. There was no endpoint rebind, log
compaction, TUN interface error, pump error, or final live/outstanding
ownership.

## `Stopped(0)` Classification

The forward data relay ended at the iperf command boundary with:

- one raw write error: `ConnectionReset: Stopped(0)`;
- one canonical `tcp-d16-relay-close` with
  `terminal_reason=remote_write_failed`;
- `writer_progress_bytes=23,105,896`;
- `queue_queued=0`, `queue_leased=0`, and `queue_reserved=0`;
- no terminal pending reap or stranded endpoint ownership.

The same Endpoint remained alive and the subsequent reverse and DNS smoke
completed. This matches the already accepted Knife15 iperf forward close-tail
class. The summary intentionally reports `internal_failure_scan: REVIEW`; the
raw error plus canonical close plus derived handle-close produce three text
matches but represent one logical event.

A manual instruction that grepped for any `remote_write_failed` and converted
it directly into `do not run M0` was therefore incorrect. `REVIEW` requires
phase-aware correlation; it is not an automatic stop. Formal receiver
continuity remains enforced by the direction-aware M0 JSON validators.

## Other Operational Evidence

The first direct attempt `...033055` contains
`interrupt - the client has terminated by signal Interrupt`; it is an operator
abort, not a path discriminator. The next direct directory `...033233` passed
and completed at `03:37:37Z`, but it expired after the TUN was stopped.

Smoke completed at `03:40:18Z`. Another VPN's `utun1024` became the Exit route
at `03:41:28Z`, after smoke but before snapshot/stop, then became the Target
route during stop. It did not cause the `Stopped(0)` event, but it invalidates
the final physical counter delta and must not recur during formal M0 evidence.

## Decision And Next Action

No data-plane setting, workload rate, SLI, or runner classification changes.
The next formal attempt must:

1. keep every other VPN/proxy TUN disabled through Knife15 `stop`;
2. reuse the valid baseline only after a fresh 300-second direct discriminator
   passes without interruption;
3. start, smoke, and enter M0 within the 15-minute direct-evidence window;
4. retain a lone equivalent boundary `Stopped(0)` as `REVIEW` evidence without
   aborting M0;
5. stop for a smoke command failure, a non-equivalent transport/lifecycle
   error, nonzero stranded ownership, or a formal receiver SLI failure.

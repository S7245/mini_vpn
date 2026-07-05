# Knife14bn A/B Evidence Spec

Date: 2026-07-05

## Stage Goal

Run a same-window VPS A/B for `09bb67c` and `3d06bea` to determine whether the
Knife14bm no-data-stream failure is a behavior regression from the
recent-pressure latch or run-to-run stream/send-side variance.

## Non-Goals

- Do not change Rust data-plane behavior in this stage.
- Do not tune stale pool, iperf3, sing-box, egress pacing, TUN queue length, or
  congestion control.
- Do not treat a failed startup/auth run as throughput evidence.
- Do not store TUIC credentials, passwords, private keys, or derived secret
  hashes in docs, logs, learnings, or summaries.

## Invariants

- Both commits must run the same reverse-first P1 command shape:
  `RUN_REVERSE_FIRST_P1=1`, `STOP_AFTER_REVERSE_FIRST_P1=1`, and
  `SERVER_EVIDENCE_CHECK=1`.
- Both commits must use the same host topology: `.27` client, `.33` TUIC exit,
  `.77` iperf3 target.
- Both commits must use the same product defaults for this branch unless the
  commit itself changes them.
- Direct `.27 -> .77` and `.33 -> .77` baselines must be captured for each run.
- Server evidence must include the `.77` target iperf3 journal and `.33` TUIC
  inbound/direct outbound lines for the probe window.
- `.27` sudo must be handled only in a true writable TTY. No sudo password may
  be placed in commands, scripts, docs, or summaries.

## Classification

### Code Regression

Classify as a likely `3d06bea` regression only if:

- `09bb67c` returns to the Knife14bl pressure/tail-collapse shape, and
- `3d06bea` repeats the no-data-stream shape under similar direct baselines, and
- `.33` has no current TUIC fail-auth/startup failure, and
- `.77` target evidence shows the low sender bytes only for `3d06bea`.

### Run Variance Or External Stream Readiness

Classify as stream/send-side variance if:

- both commits show low target sender bytes with quiet local egress, or
- the failing shape flips between commits without corresponding local code-path
  activation, or
- startup/auth or service health prevents a clean comparison.

### TUN Pressure Branch Still Active

Classify as TUN/downlink pressure if either commit reaches useful throughput and
then reports:

- `tun_tx_dropped_delta > 0`,
- `downlink_backpressure pause_edges > 0`, or
- terminal pending / pending-at-close bytes in the pressure window.

## Acceptance

The stage is accepted when it produces:

- one bundle for `09bb67c` and one bundle for `3d06bea`;
- parsed sender/receiver, target sender, TUN/downlink, terminal pending, QUIC,
  and TUIC stream-readiness summaries for both runs;
- a written result explaining which classification applies;
- updated `.learnings/LEARNINGS.md` and `.learnings/ERRORS.md` if the run
  changes future debugging behavior.

# Knife14ba plan - TUIC stream first-byte and read-gap diagnostics

Date: 2026-07-04

## Design Tree

Evidence to preserve from Knife14az:

- Direct client-target and exit-target paths are healthy.
- mini_vpn local downlink capacity was not exhausted in the failed clean window.
- The failed reverse stream produced tiny remote-to-local bytes and sparse data
  arrival.

Branches this stage must distinguish:

1. TUIC TCP stream opens but remote bytes arrive only after a large first-byte
   delay.
2. First bytes arrive, but subsequent remote reads have large gaps or only occur
   after local finish.
3. Relay live timing shows no local/QUIC pressure, implying an upstream/server
   stream scheduling or protocol lifecycle question.
4. Diagnostics contradict each other, requiring architecture review rather than
   another suffix patch.

Rejected branches unless new evidence appears:

- local downlink tx-queue drain as the immediate Knife14az limiter;
- terminal pending or close-time pending as the hidden loss point;
- TUN qdisc/syscall drops;
- clean-window QUIC loss/congestion;
- stale TUIC pool slots, iperf3, sing-box, and egress pacer tuning.

## Task Plan

1. Add Knife14ba spec/plan docs.
2. TDD relay timing summary fields in `src/client_tun.rs`.
3. TDD TUIC TCP stream diagnostic line formatting in `src/tuic.rs`.
4. Implement behavior-neutral timing:
   - relay `first_remote_read_ms`, `current_remote_read_gap_ms`,
     `max_remote_read_gap_ms`;
   - TUIC stream `first_rx_ms`, `max_read_gap_ms`, total bytes/reads, and close
     summary.
5. Update `scripts/knife14b-lowrtt-probe.sh` to parse the new lines and produce
   summary + attribution labels.
6. Update suite self-test or invocation path so `.33 -> .77` preflight can be
   carried into the acceptance report for this stage.
7. Run local gates and update `.learnings`.
8. Commit/push diagnostics.
9. Run scoped VPS acceptance and record results in a separate results commit.

## Risk Controls

- Keep all Rust changes behavior-neutral: no altered timeouts, buffers, queues,
  close paths, flow control, or reconnect paths.
- Add parser fields in a backward-compatible way.
- If any local test fails after instrumentation, analyze the failure and state a
  correction plan before further behavior changes.
- If VPS acceptance fails again, do not change behavior until the new
  first-byte/read-gap evidence has been written to a result doc.


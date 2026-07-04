# Knife14aw spec - send-window and global_rx queue diagnostics

Date: 2026-07-04

## Grounding

Knife14av made per-probe summaries wait for a bounded post-iperf close-tail
window. The parser fix worked: every raw `tcp-handle-close pending>0` line in
the `.27` run appeared in the matching probe summary.

The throughput target still failed. The clean reverse-first P1 window reached
only `11.4/10.1 Mbit/s` while direct paths were healthy and the tunnel window
had no clean QUIC loss/congestion, no local write pressure, no `send_slice`
zero/error, no TUN flush failure, and no immediate flush deferral.

The active signals were local downlink backpressure, local TUN egress drops,
and terminal pending at close.

## Problem

The current diagnostics cannot distinguish these cases:

1. smoltcp tx buffer is full but the TCP state still allows sending;
2. smoltcp TCP state has reached a terminal no-send state;
3. local app receive behavior is filling or not draining the socket queues;
4. relay `global_rx` has backlog even though `send().await` waits less than the
   pressure threshold;
5. terminal pending is merely post-close accounting versus a symptom of an
   earlier receive-window stall.

`global_rx_pressure=0` is especially weak evidence because the channel capacity
is large. A relay can enqueue many `64 KiB` payloads without crossing the wait
threshold.

## Goal

Add behavior-neutral diagnostics that expose, per reverse/downlink probe:

- smoltcp `send_capacity`, `send_queue`, `recv_queue`, `may_send`, and
  `may_recv` during downlink flush attempts;
- consecutive `can_send=false` streak and largest pending backlog during that
  state;
- close-time socket queue/window snapshot;
- relay `global_rx` channel used/max capacity high water.

## Non-Goals

- Do not change downlink flush semantics.
- Do not change close-drain, reap, or grace behavior.
- Do not change egress pacing defaults.
- Do not change TUN queue length, TUIC TCP pool, iperf3, sing-box, or VPS
  service configuration.
- Do not store secrets or credential-bearing logs in docs or learning memory.

## Invariants

- Existing `tcp-downlink-flush` fields remain parseable with their current
  names.
- Older logs without the new fields still summarize with zero-valued new
  fields.
- Diagnostics stay behind `MINI_VPN_TCP_DIAG`.
- The hot data path records only cheap public smoltcp/Tokio counters.
- No private smoltcp internals or unstable crate assumptions are introduced.

## Acceptance

Local:

- focused Rust tests for downlink send-window aggregation and relay queue
  accounting;
- `cargo test --lib client_tun`;
- low-RTT probe self-test;
- US-client suite self-test;
- shell syntax checks;
- `git diff --check`.

VPS:

- scoped `.27` reverse-first P1 run from the pushed commit;
- the per-probe summary must include `send_window_samples`,
  `send_capacity_min/max`, `send_queue_max`, `recv_queue_max`,
  `no_send_streak_max`, `no_send_pending_max`, and `global_rx` queue watermarks;
- if throughput remains in the `10-20 Mbit/s` band, the report must show
  whether the local limiter is smoltcp send-window closure, relay queue backlog,
  TUN egress drops after successful smoltcp acceptance, or a still-missing
  branch.

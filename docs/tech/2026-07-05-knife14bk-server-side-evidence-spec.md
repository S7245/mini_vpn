# Knife14bk Server-Side Evidence Spec

Date: 2026-07-05

## Stage Goal

Knife14bk must turn the remaining TUIC stream starvation branch into an
end-to-end send-side timeline. The next scoped reverse-first P1 bundle must show
whether `.77` sent only a small amount, whether `.33` accepted and forwarded the
TCP stream, and whether mini_vpn saw a periodically polled but mostly idle TUIC
stream.

## Grounding

Knife14bj on `4dc79af` showed:

- reverse P1 sender/receiver: `0.210/0.019 Mbit/s`;
- `.27 -> .77` and `.33 -> .77` baselines around `279-282 Mbit/s`;
- no sampled `.33 fail auth`;
- no local TUN drops, downlink/global_rx/local_write pressure, or QUIC
  loss/congestion/blocking;
- data stream `data_pending_gap_max_ms=20516`,
  `data_poll_gap_max_ms=5000`, and `data_rx_bytes_max=137520`.

Manual `.77` journal inspection after the run showed the corresponding iperf3
reverse test also sent only `768 KBytes / 210 Kbit/s`, with long zero-throughput
intervals. That means the next evidence must be captured in the suite, not left
as a manual side channel.

## Non-Goals

- Do not change mini_vpn data-plane behavior in this stage.
- Do not tune sing-box, iperf3, egress pacing, TUN queue length, stale pool, or
  buffer sizes.
- Do not print TUIC UUIDs, passwords, private keys, or full configs.
- Do not make target SSH mandatory for ordinary smoke runs.

## Required Evidence

When explicitly enabled, the suite should capture bounded server-side evidence
around each scoped probe:

- `.33` sing-box log lines relevant to the client/target/TUIC stream:
  inbound TUIC, outbound direct, stream cancel/close, error, and fail-auth
  lines, with UUID/password redaction.
- `.77` iperf3 service journal for the probe time window, including accepted
  connection, per-second sender/receiver lines, final sender/receiver totals,
  retransmits, and cwnd.
- Client/exit/target time context sufficient to correlate these windows.

## Acceptance

Local:

- suite self-test covers the new server-evidence help/env shape;
- shell syntax passes;
- no new data-plane Rust behavior changes;
- `git diff --check` passes.

VPS:

- reverse-first P1 bundle includes a server-evidence artifact;
- artifact can identify whether target iperf3 sent only a small amount during
  the tunneled reverse run;
- artifact includes `.33` TUIC/outbound lines for the same window and reports
  whether `fail auth` appeared.

## Stop Conditions

- If `.77` shows low sender throughput matching mini_vpn's low sender result,
  stop changing local receive/write paths and inspect ACK/window/stream-forward
  behavior.
- If `.77` shows high sender throughput while mini_vpn receives little, inspect
  `.33` forwarding and TUIC/quinn receive behavior.
- If `.33` shows `fail auth`, classify service/config/time/auth evidence before
  any data-plane patch.

# Knife14be spec - receive-window A/B

Date: 2026-07-05

## Grounding

Knife14bd validated that adaptive downlink backpressure watermarks are active:
with `MINI_VPN_TCP_TX_BUFFER_BYTES=1048576` and unset backpressure env values,
startup logged `high=1048576B low=262144B`.

That did not clear clean reverse-first throughput:

- iperf receiver: `26.2 Mbit/s`;
- clean-window QUIC loss/congestion delta: `0`;
- clean-window TUN drop delta: `0`;
- TUN flush failures/deferred: `0/0`;
- local/global write pressure: `0/0`;
- data stream first read: `3ms`;
- data stream max read gap: `3826ms`;
- `send_queue_max=1048576`;
- `terminal_pending_reap=1058416`.

The remaining clean branch is local TCP tx-buffer / receive-window capacity or
local TCP/TUN drain cadence. A larger tx buffer is a cheap discriminating test
because the binary already supports up to `16MiB` TCP socket buffers and
backpressure defaults now scale with the configured tx buffer.

## Goal

Run a scoped receive-window A/B on the same US-client path by raising only the
local TCP tx buffer to `4MiB`, leaving downlink backpressure on `<auto>`.

## Hypothesis

If the clean reverse-first bottleneck is primarily receive-window capacity, then
raising the local TCP tx buffer from `1MiB` to `4MiB` should materially improve
useful receiver throughput without introducing clean-window QUIC loss,
TUN drops, flush failures, or unbounded pending.

If throughput does not materially improve and the terminal tail merely scales
with the larger buffer, the next root is local TCP/TUN drain cadence rather than
window size.

## Non-goals

- Do not change Rust code in this slice.
- Do not change close-drain, reap, TUIC TCP pool, sing-box, iperf3, egress
  pacing, or TUN queue length.
- Do not run noisy forward-first/full sweeps for the root-cause decision.
- Do not change product defaults based on a single A/B.

## Invariants

- `MINI_VPN_TCP_RX_BUFFER_BYTES` remains `1048576`.
- `MINI_VPN_TCP_TX_BUFFER_BYTES` is explicitly overridden to `4194304`.
- `MINI_VPN_DOWNLINK_BACKPRESSURE_HIGH_BYTES` and
  `MINI_VPN_DOWNLINK_BACKPRESSURE_LOW_BYTES` remain unset/empty so the runtime
  derives `high=4194304B low=1048576B`.
- `RUN_REVERSE_FIRST_P1=1` and clean reverse-first P1 is the decision window.
- Service preflight for `.33` and `.77` must pass before the expensive run.

## Acceptance

Local:

- spec/plan committed before VPS;
- suite self-test passes locally or on `.27`;
- no code changes are made before the A/B result.

VPS:

- `.27 -> .77` and `.33 -> .77` direct forward/reverse baselines are healthy;
- startup log shows `TCP socket buffers: rx=1048576B tx=4194304B`;
- startup log shows `TCP downlink backpressure: high=4194304B low=1048576B`;
- clean reverse-first P1 improves materially over Knife14bd's `26.2 Mbit/s`
  receiver result, or it decisively falsifies the receive-window hypothesis;
- clean-window attribution includes no QUIC loss/congestion, no TUN egress
  drops, no TUN flush failures, no `send_slice_zero`, and no send-slice errors.

## Decision Rule

- If receiver throughput improves materially and clean-window loss/drop signals
  remain quiet, plan the smallest code/config change that makes the larger
  receive window explicit and operationally bounded.
- If throughput stays in the same band or terminal pending simply scales toward
  `4MiB`, stop increasing buffers and design the next stage around local
  TCP/TUN drain cadence instrumentation or behavior.

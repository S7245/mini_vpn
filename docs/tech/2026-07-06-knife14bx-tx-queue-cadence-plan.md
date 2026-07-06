# Knife14bx Tx Queue Cadence Plan

Date: 2026-07-06

## Grounding

Knife14bw added behavior-neutral starvation diagnostics after Knife14bv's
close-egress drain run produced a no-data result. The new VPS run moved the
evidence back into the repeatable low-average tier: reverse-first P1 stayed at
about `19.8/18.9 Mbit/s`, direct `.27 -> .77` and `.33 -> .77` baselines were
healthy, no current-window TUIC `fail auth` appeared, and QUIC
loss/congestion/blocking, TUN drops, send-slice failures, terminal pending reap,
and local/global_rx pressure were all clean.

The key new discriminator is local cadence: `tcp_reverse_window` showed useful
TUIC stream bytes, app pending stayed `0`, but downlink backpressure still
paused/resumed around smoltcp `send_queue` pressure. The previous Knife14bt and
Knife14bu runs already rejected both larger default high/low watermarks and a
simple 25ms durable pressure hold as throughput fixes.

This stage treats app-owned pending bytes and already-accepted smoltcp
`send_queue` bytes as separate pressure surfaces. App pending remains hard
bounded by the conservative high/low values. Tx-queue-only pressure gets a
bounded headroom band before the main loop pauses `global_rx`, so the local TCP
window does not oscillate at the soft high watermark when there is no app
backlog left to lose.

## Stage Goal

Make reverse downlink receive cadence less stop/go when the only visible
pressure is smoltcp `send_queue`, while preserving bounded memory and keeping
app-owned pending data under the existing conservative high/low guard.

## Non-goals

- Do not change stale TUIC pool behavior, iperf3, sing-box, QUIC congestion,
  TUN qlen, or the egress pacer default.
- Do not lengthen the Knife14bu egress pressure hold.
- Do not relax app-owned `downlink_pending` bounds.
- Do not store VPS passwords, TUIC credentials, private keys, or one-off noisy
  logs in repository files.

## Design Tree

- Larger high/low default: rejected by Knife14bs/Knife14bt because it kept the
  low-average stop/go profile and inflated local queueing.
- Longer pressure hold: rejected by Knife14bu because it removed drops but made
  pause churn and throughput worse.
- Close/reap drain: rejected as the throughput root by Knife14bv/Knife14bw; the
  first loss point is before terminal cleanup in the clean low-average shape.
- Tx-queue headroom: plausible because Knife14bw shows app pending `0`, useful
  TUIC bytes, bounded tx_queue pressure, and pause/resume churn. This branch is
  small enough for deterministic TDD and one scoped VPS run.

## Planned Behavior

- Keep `DEFAULT_DOWNLINK_BACKPRESSURE_HIGH_BYTES = 512 KiB` and
  `DEFAULT_DOWNLINK_BACKPRESSURE_LOW_BYTES = 128 KiB`.
- If app pending reaches high, pause immediately.
- If already paused and app pending remains above low, stay paused.
- If app pending is clean and only tx_queue is high, pause only at a derived
  hard tx-queue threshold: `high + (high - low)`.
- Once tx-queue-only pressure caused a pause, resume when tx_queue drains back
  to the soft high watermark.

For the default config this means app pending still pauses at `512 KiB`, while
tx-queue-only pressure has a hard pause threshold of `896 KiB` and resumes at
`512 KiB`.

## TDD Plan

1. Add a focused test that expects tx-queue-only pressure at the soft high
   watermark not to pause `global_rx`.
2. In the same test, assert that tx-queue-only pressure still pauses at the
   derived hard threshold and resumes at the soft high watermark.
3. Keep the existing pending high/low hysteresis test unchanged to prove app
   backlog protection is not relaxed.
4. Update the old tx_queue pressure test after the RED failure confirms it
   encoded the now-rejected immediate-pause behavior.

## Local Acceptance

- The new tx_queue cadence test fails before the implementation and passes
  after it.
- Existing pending, TUN egress feedback, close-drain, and parser tests stay
  green.
- `cargo test --lib`, release build, shell self-tests/syntax checks, and
  `git diff --check` pass. Repo-wide `cargo fmt --check` remains out of scope
  because the repository has known unrelated rustfmt drift.

## VPS Acceptance

Run one scoped `.27 -> .33 -> .77` reverse-first suite after code commit/push.
The result should show:

- reverse-first P1 leaves the 10-20 Mbit/s low-average tier or produces a new
  clear discriminator;
- pause/resume edges and `tun_flush_deferred` do not worsen materially;
- no TUN drops, QUIC loss/congestion/blocking, send-slice failures, terminal
  pending reap, or unexpected `.33` TUIC auth/config failure.

If `.33` reports `fail auth`, first inspect time sync, sing-box service/config,
and mini_vpn TUIC env/config before treating it as a data-plane regression.

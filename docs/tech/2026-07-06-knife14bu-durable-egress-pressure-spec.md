# Knife14bu Durable Egress Pressure Spec

## Grounding

Knife14bt proved the core auto default is now conservative
`524288/131072`, but reverse-first P1 still failed at `18.4/17.4 Mbit/s`.

The important clean signals were:

- direct `.27 <-> .77` and `.33 <-> .77` baselines were healthy;
- QUIC loss, congestion, and blocking deltas were zero;
- `send_slice_zero=0`, `send_slice_errors=0`, and `tun_flush_failures=0`;
- `terminal_pending_reap=0`, so pending bytes were not hidden by reap;
- `downlink_backpressure=26/26`, `max_tx_queue_bytes=586083`,
  `tun_tx_dropped_delta=172`, and data stream gaps reached `3873ms`;
- logs repeatedly showed `max_tx_queue >= high` followed immediately by
  `max_pressure=0` resume.

The remaining branch is local egress pressure becoming invisible immediately
after `iface.poll + flush_tx` moves bytes from smoltcp into Linux TUN/qdisc.
The current gate only sees app pending plus smoltcp `send_queue`, so it can
resume remote stream reads before local egress has had time to drain.

## Design Tree

1. Tune scripts or acceptance env again.
   - Rejected. Knife14bt confirmed `<auto>` and the startup high/low from the
     binary.

2. Change sing-box, iperf3, stale pool, QUIC, or TUN qlen.
   - Rejected for this slice. Current evidence keeps those branches clean.

3. Lower or raise backpressure high/low again.
   - Rejected. Both `1048576/262144` and `524288/131072` produced low-average
     stop/go on the current topology.

4. Hold recent local egress pressure briefly after a high send queue.
   - Accepted. This keeps the downlink gate from treating a just-flushed burst
     as immediately clean while TUN/qdisc egress is likely still outstanding.

## Stage Goal

Add a small, deterministic core pressure hold: after raw downlink pressure
reaches the configured high watermark, effective downlink pressure remains at
least that high for a short bounded interval even if the next raw snapshot has
already dropped to zero.

## Non-Goals

- Do not change TUIC auth, sing-box, iperf3, stale pool handling, TUN qlen, or
  script config normalization.
- Do not change terminal close/reap policy in this patch.
- Do not add a long sysfs-sample-scale pause.
- Do not remove existing TUN drop feedback; this is a pre-drop local pressure
  gate, not a replacement for drop attribution.

## Invariants

- Raw pressure at or above high still pauses immediately.
- While paused, held pressure keeps the gate paused only until the bounded hold
  expires.
- Once the hold expires and raw pressure is below/equal low, ordinary low
  watermark resume works.
- A new high-pressure raw observation extends the hold.
- TUN drop feedback still uses current/recent pressure and still resumes on
  low pressure.

## Acceptance

Local:

- A focused Rust test first fails, then proves held pressure keeps
  `next_downlink_backpressure(true, ...)` paused after a high-to-zero raw
  transition.
- Existing downlink backpressure tests pass.
- Existing TUN egress feedback tests pass.
- Full `cargo test --lib` and `git diff --check` pass.

VPS:

- Scoped reverse-first P1 starts with `high=524288B low=131072B`.
- Expected improvement: fewer zero-throughput seconds and lower data-stream
  read/pending gaps than Knife14bt.
- Required safety: `terminal_pending_reap=0`, no send-slice errors, no TUN
  flush failures, and no QUIC loss/congestion/blocking regression.

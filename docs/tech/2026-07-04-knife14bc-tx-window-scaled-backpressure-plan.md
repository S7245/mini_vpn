# Knife14bc plan - tx-window scaled downlink backpressure

Date: 2026-07-04

## Design tree

1. Preserve terminal pending longer.
   Rejected for this slice. Knife14ba shows the useful receiver bytes had
   already been accepted by smoltcp before local close. The terminal pending is
   the post-close tail, not the main 28.7 Mbit/s limiter.

2. Remove tx-queue backpressure.
   Rejected. Knife14aw showed full tx queues and TUN drops when tx-queue
   pressure was invisible. Backpressure is still needed as a bounded feedback
   edge.

3. Tune egress pacing or TUN queue length.
   Rejected. Knife14ar/Knife14ba clean windows had `tun_flush_deferred=0`,
   `tun_flush_failures=0`, and clean TUN drop deltas.

4. Scale tx-queue backpressure defaults with the configured TCP tx buffer.
   Selected. The current fixed `512KiB` high watermark cuts the effective
   receive window in half when `.env` raises the local TCP tx buffer to 1MiB.
   Scaling defaults preserves bounded feedback while giving the data stream the
   receive window the socket was configured to provide.

## Tasks

1. Add a focused failing test for adaptive default backpressure from tx buffer.
2. Implement adaptive defaults while preserving explicit env overrides.
3. Confirm startup still logs the selected high/low values.
4. Run focused Rust tests and script self-tests.
5. Update learning memory.
6. Commit and push.
7. Run scoped VPS reverse-first acceptance.

## Risk checks

- High concurrency memory remains bounded by the already configured per-socket
  TCP tx buffer; this patch changes when global_rx pauses, not the socket buffer
  allocation itself.
- Explicit low >= high repair stays active.
- Existing test-only `parse_downlink_backpressure_config` behavior remains
  available for legacy fixed-default tests.
- If the next VPS run shows TUN drops or QUIC congestion returning, revert to
  evidence and do not keep tuning suffixes.

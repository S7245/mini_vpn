# Knife14eu ACK/window discriminator spec

Date: 2026-07-07

Branch: `codex/knife14d-downlink-reap-open`

## Background

Knife14er-es-et separated two failure families:

- local downlink pressure / headroom oscillation, where pending and send queue
  grow while throughput stays low;
- no-pressure reverse no-data, where local egress is clean but the TUIC TCP
  stream has long read/pending gaps and the target sender appears starved.

Knife14et produced the clearest discriminator:

- direct and exit-to-target baselines were healthy;
- `downlink_backpressure pause_edges=0 resume_edges=0 max_pressure_bytes=0`;
- `pending_total_max=0`, `headroom_deferred_bytes=0`;
- `read_credit_limit_bytes_min=65536`;
- data stream received only `210912B`;
- `data_read_gap_max_ms=20500`;
- attribution included `target_sender_stalled+tuic_stream_starved`.

This means another local pressure threshold tweak is unlikely to finish the
last 3%.

## Goal

Add a code-level discriminator that can tell whether the next reverse-first P1
failure is caused by:

1. local FIN / half-close ordering starving the remote sender;
2. insufficient TUN RX ACK/window drain while local egress is otherwise clean;
3. TUIC stream read/pending wakeup behavior despite QUIC frames arriving;
4. true local egress pressure returning.

## Non-goals

- Do not retune TUN qlen, QUIC MTU/PLPMTUD, sing-box, iperf3, stale pool, or
  global egress pacer.
- Do not claim throughput acceptance from this stage unless VPS receiver is
  actually `100+ Mbit/s`.
- Do not write secrets or sudo passwords to commands, scripts, docs, logs, or
  learning memory.
- Do not hide the Knife14et regression by committing it as a successful
  performance fix.

## Required observability

The relay live/close line for each flow must expose:

- local finish timing:
  - number of local finish events;
  - first local finish after first remote read in milliseconds;
  - remote bytes and reads after local finish;
  - maximum remote read gap before local finish;
  - maximum remote read gap after local finish.
- ACK/window drain pressure:
  - number of ACK drain hint attempts already exists;
  - whether hints happen while writer is done;
  - whether hints produce remote progress after local finish.

The existing attribution parser should then be able to distinguish:

- `late_remote_after_local_finish` from normal close-tail bytes;
- `tuic_stream_read_gap` before local finish;
- `tuic_stream_read_gap` after local finish.

## Optional A/B guardrail

If the discriminator shows remote read gaps only after local finish, add a
separate opt-in experiment flag in a later stage. The flag should delay
propagating local FIN to the TUIC write half while the reverse data stream is
still receiving bytes, with a bounded timeout. That behavior change is not part
of this spec.

## Acceptance for this stage

Local:

- focused unit tests for the new timing counters pass;
- `cargo test --lib --quiet` passes;
- `cargo clippy --all-targets --features harness --quiet -- -D warnings`
  passes;
- `cargo build --release --quiet` passes;
- script self-tests pass.

Remote:

- `.27` focused tests and release build pass.

VPS discriminator:

- Run one safe1200 reverse-first P1 with explicit exit-to-target preflight
  settings.
- If receiver reaches `100+ Mbit/s`, record acceptance.
- If it fails, stop and classify the result into FIN-ordering, TUIC read
  wakeup, TUN ACK drain, local pressure, or environment.

# Knife14q spec - do not fail TCP relays on normal uplink write backpressure

## Context

`480c3e8` (`knife14p`) added downlink pending backpressure. The US-client suite
at `/tmp/mvpn_knife14p_usclient_suite_20260702_143215.tar.gz` shows that the
stage did not meet the real tunnel goal:

- forward P=1 briefly reaches useful throughput, then falls into long zero-rate
  windows;
- forward P=2 times out immediately;
- reverse P=1 is only Kbit/s-class, and reverse P=2/4/8 time out;
- `global_rx_pressure_events=0` everywhere, so the new downlink high/low
  watermark did not trigger;
- the client log still contains `remote_write_timeout` for 1160-byte
  local-to-remote writes.

The high-confidence code-review finding is that `run_relay_writer` wraps every
`writer.write_all(&payload)` in a 5s timeout. For TUIC/QUIC streams, a pending
write can simply mean ordinary QUIC flow-control or path backpressure. Treating
that as relay failure closes a healthy TCP flow and prevents TCP backpressure
from propagating naturally to the local application.

## Goal

Let local-to-remote TCP writes wait under normal stream backpressure without
closing the relay after a fixed 5s per-chunk deadline.

## Non-goals

- Do not change TUIC connection pooling.
- Do not change downlink pending high/low defaults.
- Do not tune congestion control, MTU, or VPS service configuration.
- Do not solve every reverse-direction performance issue in this stage unless
  it falls directly out of the per-write timeout fix.

## Required behavior

- A pending `write_all` must not emit `remote_write_timeout`.
- A pending writer must not block remote-to-local reads; the split relay reader
  must still make progress.
- A truly idle relay is still bounded by `RELAY_IDLE_TIMEOUT`.
- Once the relay closes, writer task cleanup is still bounded and may be aborted
  if shutdown/stop wedges.
- Direct write errors still close the relay with `remote_write_failed`.

## Acceptance

Local gates:

- `cargo test --lib client_tun`
- `cargo test`
- `cargo clippy --all-targets -- -D warnings`
- `git diff --check`

Next US-client run should show:

- no `remote_write_timeout` during normal throughput probes;
- forward P=1/P=2 no longer collapse because a single 1160-byte write waited
  more than 5s;
- if reverse still underperforms, logs should point to downlink/half-close/reap
  behavior rather than per-write uplink timeout.

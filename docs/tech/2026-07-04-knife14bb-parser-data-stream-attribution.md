# Knife14bb parser data-stream attribution

Date: 2026-07-04

## Stage goal

Fix the Knife14ba parser caveat without changing mini_vpn runtime behavior:
stream read-gap attribution must distinguish data streams from tiny control
streams, so an idle iperf control stream does not get reported as the reverse
throughput root.

## Non-goals

- No Rust data-plane behavior changes.
- No egress pacer, TUIC pool, sing-box, iperf3, or TUN queue tuning.
- No VPS rerun for this parser-only stage.

## Invariants

- Keep global maxima visible for diagnosis:
  - all-stream TUIC first byte / read gap / close gap;
  - all-relay remote first read / read gap.
- Add data-stream maxima beside the global values.
- Assign reverse TCP read-gap attribution from data-stream maxima only.
- Preserve the "no first remote read yet" first-byte label, because it catches
  the true zero-downlink case where no data stream can be classified yet.

## Data-stream threshold

The parser classifies a stream/relay handle as data-bearing when observed
received bytes are at least `65536`. This filters tiny control streams such as
the Knife14ba `rx_bytes=343` control stream while still preserving attribution
for low-throughput data streams that only reach tens of KiB.

## TDD

Added a low-RTT parser self-test with the Knife14ba shape:

- control stream: `rx_bytes=343`, `max_read_gap_ms=30147`;
- data stream: `rx_bytes=110790062`, `max_read_gap_ms=3763`;
- expected summary:
  - global `max_read_gap_ms=30147` remains visible;
  - `data_streams=1`;
  - data read-gap maxima are `3763`;
  - attribution does not include `tuic_stream_read_gap` or
    `relay_remote_read_gap`.

## Verification

- `scripts/knife14b-lowrtt-probe.sh --self-test`
- `scripts/knife14b-usclient-tunnel-suite.sh --self-test`
- `bash -n scripts/knife14b-lowrtt-probe.sh`
- `bash -n scripts/knife14b-usclient-tunnel-suite.sh`

## Result

The parser now reports both global and data-stream timing fields:

- `relay_remote_timing ... data_streams=... data_first_read_max_ms=...
  data_max_read_gap_ms=... data_rx_bytes_max=... data_rx_min_bytes=65536`
- `tuic_tcp_stream ... data_streams=... data_first_rx_max_ms=...
  data_read_gap_max_ms=... data_close_gap_max_ms=... data_rx_bytes_max=...
  data_rx_min_bytes=65536`

Reverse TCP attribution now uses the data-stream fields for
`tuic_stream_first_byte_slow`, `tuic_stream_read_gap`,
`relay_remote_first_byte_slow`, and `relay_remote_read_gap`, except that
`relay_remote_first_byte_slow` still fires on the no-first-read timer branch.

## Next

Start the behavior-focused Knife14bb/Knife14bc slice on local tx-queue /
receive-window / terminal pending:

- keep the stream timing diagnostics as guards;
- add deterministic accounting/tests for tx-queue pressure and close-time
  terminal pending;
- investigate why clean reverse-first accumulates about `524-574 KiB` in local
  send queue and reaps about `542 KiB` terminal pending despite quiet QUIC/TUN
  loss signals.

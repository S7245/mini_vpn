# Knife14 H10d16 Shoes Composite Gate A Results

Date: 2026-07-13 (VPS local/UTC date)
Accepted source: `79b41b3fe399c9cbaeeedbe907f1d04976d78a1f`

## Verdict

The approved composite Gate A passed against Shoes `v0.2.7` / Quinn `0.11.9`
on the independent `.111:8443` Exit.

- A-capacity completed the exact `20s` reverse-first P1 at `192/188 Mbit/s`
  sender/receiver. All 20 intervals carried data and the report classified the
  shape `stable_high` with no tail collapse or local pressure.
- The timed boundary produced exactly one approved
  `local_to_remote/local_socket_terminal` data-flow close. The D16 ledger
  released one bounded `524288B` reservoir and reported one bounded `27840B`
  terminal smoltcp send queue. No other terminal cause occurred.
- After the flow became quiet, A-clean transferred exactly `64 MiB` at
  `179/179 Mbit/s`, observed remote EOF only after the owned path drained, and
  closed through `clean_queue_lifecycle` with every queue, permit, pending,
  terminal-drop, and close-egress counter at zero.

This satisfies the architecture-spec amendment that separates abort-capable
timed capacity from fixed-byte clean EOF under one AND gate. Gate B is now
unlocked. This result does not yet establish a stable `170 Mbit/s` median or
product-regression readiness.

## Source And Local Gate

The direct-probe terminal classifier was first corrected by TDD at `79b41b3`.
It accepts a timed capacity result only with complete iperf JSON, a strict
floor, nonzero intervals, two Connect relays, and either two clean relays or
one clean plus one typed terminal reset with its cause preserved. Open/auth or
unexpected failures and cause-less resets remain failures.

The captured Shoes capacity evidence replayed as:

```text
receiver_mbps=192.665956
intervals=20
zero_intervals=0
min_interval_mbps=153.099296
terminal=OneReset
```

Focused classifier tests passed `7/7` with one ignored network test. The full
library passed `599/599` with two ignored tests, and
`cargo check --all-targets` passed. Code review found no product hot-path,
platform, ownership, actor-exclusivity, or EOF change in this test-only slice.

## Clean Deployment

The `.27` build came from a full Git bundle cloned into a clean detached
worktree at the exact accepted commit. Both runner self-tests and the release
build passed.

```text
source_commit=79b41b3fe399c9cbaeeedbe907f1d04976d78a1f
binary_sha256=34191751ff80f1408476f874fb66e74757384c061616380a26b54f0659ff5dff
suite_sha256=2091073867a67ba268e2be545d44d963f59ea01766ac693efd1eeb040903c806
probe_sha256=c4038c38ab8b1ee52d710d02ce2e6c9125b020247cee5faf10d7018489e9dfb0
```

The same official Shoes release used by the preceding discriminator was
redeployed with one endpoint, two worker threads, root-only one-shot FIFO
configuration/TLS material, a bounded runtime, and an independent restore
watchdog.

```text
archive_sha256=134974a4640807bb6767bf4b35f7a71637cdbbb96b8bef622ef86cf197220aac
binary_sha256=160202f6744b14ba84b40c014f3754f7811642300df58df81f1399063a202147
```

The product profile was Cubic, MTU `1200`, pool `2`, TCP RX/TX buffers
`1048576`, D16 per-flow cap `524288`, global cap `67108864`, and actor quantum
`131072`. All legacy H4/D2/D3/D5/D6/D11/D15 product candidates were disabled.

## Preflight

- `.27 -> .77` direct forward/reverse receivers: `278/274 Mbit/s`;
- `.111 -> .77` direct forward/reverse receivers: `258/266 Mbit/s`;
- `.27 -> .111` ping: `0%` loss, about `0.234ms` average;
- `.27` client QUIC socket buffers: `16 MiB` observed;
- TUN: MTU `1200`, qlen `500`, target routed through `tun0`, Exit bypassed;
- `.111` UDP input/receive-buffer/send-buffer errors: `0`.

## A-Capacity

Iperf result:

```text
sender_mbps=192.000
receiver_mbps=188.000
samples=20
overall_avg_mbps=187.550
tail_avg_mbps=178.167
tail_min_mbps=139.000
tail_collapse=0
shape=stable_high
```

The data stream delivered `469800040B`. Its active service discriminators were
all below one second:

```text
data_poll_gap_max_ms=444
data_pending_gap_max_ms=114
data_read_gap_max_ms=0
data_close_gap_max_ms=446
```

Local and transport invariants:

```text
tun_rx_dropped_delta=0
tun_tx_dropped_delta=0
actor_bypass_admitted_bytes=0
send_slice_zero=0
send_slice_errors=0
flush_tx_failures=0
pressure/drop credit debt=0
terminal_pending_reap_bytes=0
QUIC loss/congestion/blocking deltas=0
```

The iperf client boundary moved the data socket directly from `Established` to
terminal `Closed` before data remote EOF. The exact classified accounting was:

```text
d16_local_socket_terminal_events=1
d16_other_terminal_events=0
permit_terminal_drop_events=1
permit_terminal_drop_bytes=524288
close_egress_class=terminal_closed_no_send
close_egress_bytes=27840
close_egress_drain_candidate=false
queue_queued=0 queue_leased=0 queue_reserved=0 queue_closed=true
```

The byte count matches the configured one-flow D16 reservoir exactly and was
released once at the authoritative rearm boundary. The `27840B` smoltcp bytes
were already undeliverable after the local peer became terminal; attempting to
drain them would violate the socket lifecycle contract.

## A-Clean

The same process, binary, profile, tunnel, and Exit then ran
`iperf3 -n 64M -P 1 -R`:

```text
sender=64.0 MiB at 179 Mbit/s
receiver=64.0 MiB at 179 Mbit/s
tuic_data_rx_bytes=67108864
data_max_read_gap_ms=0
data_poll_gap_max_ms=629
data_pending_gap_max_ms=28
data_close_gap_max_ms=138
```

The D16 data flow reported `clean_queue_lifecycle`. Remote EOF became visible
with `pending=0` and `inflight_permit_bytes=0`, then the local socket closed
with `send_queue=0`. Its final isolated window had:

```text
pending_at_close=0
terminal_pending_reap=0
egress_at_close=0
permit_terminal_drop=0
actor_bypass=0
send_slice_zero=0
send_slice_errors=0
tun_rx_dropped_delta=0
tun_tx_dropped_delta=0
queue_queued=0 queue_leased=0 queue_reserved=0
d16_clean_close_events=2
d16_terminal_close_events=0
```

## Review Conclusion

The result confirms the approved direction rather than selecting another code
change. The timed `524288B + 27840B` terminal edge is the exact bounded case
already modeled by the architecture amendment; it is not a permit leak or an
EOF-ordering failure. The fixed-byte flow proves that natural EOF drains the
same ownership path completely. No D16 queue, actor cadence, DrainOnly,
recovery, MTU, QUIC window, pool, chunk, or self-wake optimization is justified
by this run.

The next stage is Task 12 / Gate B:

1. qualify one same-window Gate-aligned sing-box control;
2. run three exact `20s` mini_vpn reverse-first P1 repeats on the accepted D16
   build/profile;
3. require every receiver above `150 Mbit/s`, median at least `170 Mbit/s`
   (or at least `90%` of a control below `170`, while every run stays above
   `150`), and only exact classified timed-terminal accounting;
4. repeat one fixed-byte A-clean proof on the accepted build before product
   regressions.

## Artifacts And Cleanup

The locally retained suite bundle is:

```text
/tmp/mini_vpn_h10d16_shoes_gatea_79b41b3/
mvpn_knife14h10d16_shoes_composite_gatea_usclient_suite_20260713_103537.tar.gz
sha256=089acf7e6111dc1e3dac1c10abc728f12aef3df10a971c1e00635efbc521ba34
```

The bundle contains only the suite report, client log, A-capacity report, and
A-clean report. It was scanned with `env_files=0`, `private_key_markers=0`, and
`unredacted_password_assignments=0`.

The `.27` process, TUN, route override, clean worktree, bundle, and remote
output directory were removed. The `.111` Shoes service was stopped, temporary
files/FIFOs were removed, and all four temporary socket-buffer sysctls were
restored to `212992`. The `.77` iperf3 service remained active.

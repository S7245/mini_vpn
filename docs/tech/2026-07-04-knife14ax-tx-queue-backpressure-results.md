# Knife14ax results - tx-queue-aware backpressure acceptance

Date: 2026-07-04

## Build

- Code commit: `58f847d`
- VPS tag: `knife14ax_tx_queue_backpressure`
- Local bundle:
  `/tmp/mini_vpn/mvpn_knife14ax_tx_queue_backpressure_usclient_suite_20260704_215053.tar.gz`
- Extracted report directory:
  `/tmp/mini_vpn/knife14ax_tx_queue_backpressure_215053`

## Outcome

Knife14ax passed local regression but failed VPS acceptance. The clean
reverse-first P1 probe did not leave the low-throughput band; it failed harder
than the prior run:

```text
iperf3: error - unable to receive results
exit=1
```

The compact attribution for the clean reverse-first probe was:

```text
downlink_backpressure: pause_edges=0 resume_edges=0
max_tx_queue_bytes=0 max_pressure_bytes=0
downlink_flush: accepted_bytes=180681 send_queue_max=65536
global_rx_pressure: events=0 queue_used_max=1/1024
tun_tx_dropped_delta=0
quic: max_lost_bytes_delta=0 max_congestion_events_delta=0
terminal_pending_reap: events=1 bytes=41208
pending_at_close: events=1 bytes=41208
attribution: terminal_pending_reap+pending_at_close
```

The close tail for the reverse data connection showed:

```text
remote_to_global_rx_bytes=221884
pending=41208
reason=dead_slot_reap
tcp_state=Closed active=false can_send=false can_recv=false
close_pending_class=terminal_closed_no_send
terminal_pending_reap_bytes=41208
```

## Interpretation

The tx-queue-aware backpressure change was active and parser-visible, but it did
not trigger in the clean reverse-first window. The relevant `max_tx_queue` and
`max_pressure` summary fields were zero, `global_rx_paused` stayed false, and
the relay channel was not pressured. This rejects the Knife14aw hypothesis as
the clean reverse-first root for this run.

The terminal pending bytes are still a terminal accounting symptom, not a
drainable backlog. The earlier limiter is that the reverse data connection only
delivered about 0.22 MB from the remote side before the local TCP socket became
`Closed`. The event loop was mostly idle, TUN flush failures were zero, TUN
drops were zero in the clean window, and clean-window QUIC loss/congestion was
zero.

Forward P1 improved relative to earlier very low runs but carried a different
attribution:

```text
iperf_sender_mbps=71.100
iperf_receiver_mbps=62.000
attribution=quic_loss_congestion+inherited_quic_congestion+local_write_pressure+local_tun_egress_drop
```

Later reverse probes inherited QUIC congestion from the forward run, so they
are not clean evidence for the reverse-first root.

## Next Stage

Knife14ay should not continue tuning downlink backpressure watermarks. It should
add focused diagnostics and tests for the local TCP state transition that moves
the reverse data socket into `Closed` while the relay has only read a small
amount of remote data.

Minimum next checks:

- log state-transition context before `dead_slot_reap`, including previous
  observed TCP state, local FIN/RST indicators if available through smoltcp
  state, and whether any local payload or FIN was pumped after remote data
  started;
- add a deterministic test for reverse-style flows where local uplink sends a
  small request and remote downlink continues while local has no further
  payload;
- keep the tx-queue pressure diagnostics but avoid more backpressure behavior
  changes until the premature `Closed` edge is explained.
